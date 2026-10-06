//! The extraction step's ledger: what it remembers between deltas.
//!
//! - each agent's context in each conversation (`contexts`);
//! - the calls made in an output and still waiting for their result, per
//!   agent and call id, one per conversation that made a call with that id,
//!   in the order they were made (`pending`);
//! - the calls of known tools seen only among a conversation's new inputs
//!   (`history`);
//! - the results each agent was delivered (`delivered`);
//! - the deltas whose extraction committed (`done`).
//!
//! [`ExtractionLedger`] is the port; [`MemoryExtractionLedger`] the memory
//! implementation and the reference, `crate::store::PgExtractionLedger`
//! the Postgres one. The step only reads during a delta and writes every
//! change of the delta in one [`ExtractionLedger::commit`], with the
//! delta's exchange marked done.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::ids::{AccessId, AgentId, ConversationId, ExchangeId};
use crosstalk_spec::observed::message::ToolCall;
use crosstalk_spec::support::Timestamp;
use serde::{Deserialize, Serialize};

use crate::extract::ConversationContext;
use crate::store::FlowStoreError;

/// One agent's conversation: its context is its own.
pub type ContextKey = (AgentId, ConversationId);

/// The calls an agent made with one call id, by agent and id.
pub type PendingKey = (AgentId, String);

/// A history call, by conversation and call id.
pub type HistoryKey = (ConversationId, String);

/// A call made in an output, waiting for its result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCall {
    /// The conversation whose output made it.
    pub conversation: ConversationId,
    /// The context it was extracted in, which its result is extracted in
    /// too: the writes held from the call line up with the ones the result
    /// judges (`flow.extract.write-locators-from-call`) even when another
    /// result taught the context in between.
    pub context: ConversationContext,
    #[serde(with = "stored_call")]
    pub call: ToolCall,
    /// Its writes, held by the flow consumer, by locator order.
    pub writes: Vec<(Locator, AccessId)>,
}

/// An agent's delivery of one result: a digest of the call (id, name,
/// arguments) and the result (outcome, content).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveryKey {
    pub agent: AgentId,
    pub digest: [u8; 32],
}

/// Everything one delta changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LedgerChanges {
    /// Contexts the delta set, whole.
    pub contexts: BTreeMap<ContextKey, ConversationContext>,
    /// Pending lists the delta changed, whole: an empty list removes the
    /// key.
    pub pending: BTreeMap<PendingKey, Vec<PendingCall>>,
    /// History calls the delta stored (`Some`) or took (`None`).
    pub history: BTreeMap<HistoryKey, Option<ToolCall>>,
    /// Results the delta delivered.
    pub delivered: BTreeSet<DeliveryKey>,
}

/// One delta's commit: its changes, and its exchange marked done at `at`
/// (the exchange's start), which also stamps every row it writes for
/// retention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerCommit {
    pub exchange: ExchangeId,
    pub at: Timestamp,
    pub changes: LedgerChanges,
}

/// Why the ledger failed.
#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    Store(#[from] FlowStoreError),
}

/// The extraction step's ledger.
pub trait ExtractionLedger: Send + Sync {
    /// Whether the delta of `exchange` committed.
    fn done(&self, exchange: ExchangeId) -> impl Future<Output = Result<bool, LedgerError>> + Send;

    /// `agent`'s context in `conversation`, when one is stored.
    fn context(
        &self,
        agent: AgentId,
        conversation: ConversationId,
    ) -> impl Future<Output = Result<Option<ConversationContext>, LedgerError>> + Send;

    /// The calls `agent` made with `call_id`, waiting for their result, in
    /// the order they were made.
    fn pending(
        &self,
        agent: AgentId,
        call_id: &str,
    ) -> impl Future<Output = Result<Vec<PendingCall>, LedgerError>> + Send;

    /// The history call `call_id` of `conversation`.
    fn history(
        &self,
        conversation: ConversationId,
        call_id: &str,
    ) -> impl Future<Output = Result<Option<ToolCall>, LedgerError>> + Send;

    /// Whether `key`'s result was delivered to its agent.
    fn delivered(&self, key: DeliveryKey)
    -> impl Future<Output = Result<bool, LedgerError>> + Send;

    /// Apply one delta's changes and mark its exchange done, atomically.
    fn commit(&self, commit: LedgerCommit) -> impl Future<Output = Result<(), LedgerError>> + Send;

    /// Forget deliveries, contexts and done marks stamped before
    /// `horizon`.
    fn expire(&self, horizon: Timestamp) -> impl Future<Output = Result<(), LedgerError>> + Send;
}

/// The whole ledger, as the memory ledger holds it; what the model tests
/// compare.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LedgerState {
    pub contexts: BTreeMap<ContextKey, (ConversationContext, Timestamp)>,
    pub pending: BTreeMap<PendingKey, Vec<PendingCall>>,
    pub history: BTreeMap<HistoryKey, ToolCall>,
    pub delivered: BTreeMap<DeliveryKey, Timestamp>,
    pub done: BTreeMap<ExchangeId, Timestamp>,
}

impl LedgerState {
    /// Apply `commit`.
    pub fn apply(&mut self, commit: LedgerCommit) {
        let LedgerCommit {
            exchange,
            at,
            changes,
        } = commit;
        for (key, context) in changes.contexts {
            self.contexts.insert(key, (context, at));
        }
        for (key, calls) in changes.pending {
            if calls.is_empty() {
                self.pending.remove(&key);
            } else {
                self.pending.insert(key, calls);
            }
        }
        for (key, call) in changes.history {
            match call {
                Some(call) => {
                    self.history.insert(key, call);
                }
                None => {
                    self.history.remove(&key);
                }
            }
        }
        for key in changes.delivered {
            let stamped = self.delivered.entry(key).or_insert(at);
            *stamped = (*stamped).max(at);
        }
        let done = self.done.entry(exchange).or_insert(at);
        *done = (*done).max(at);
    }

    /// Forget what was stamped before `horizon`.
    pub fn expire(&mut self, horizon: Timestamp) {
        self.contexts.retain(|_, (_, at)| *at >= horizon);
        self.delivered.retain(|_, at| *at >= horizon);
        self.done.retain(|_, at| *at >= horizon);
    }
}

/// The ledger in memory. Clones share it.
#[derive(Debug, Clone, Default)]
pub struct MemoryExtractionLedger {
    state: Arc<Mutex<LedgerState>>,
}

impl MemoryExtractionLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// A copy of everything it holds.
    pub fn state(&self) -> LedgerState {
        self.with(|state| state.clone())
    }

    fn with<T>(&self, f: impl FnOnce(&mut LedgerState) -> T) -> T {
        // A poisoned lock only means a holder panicked between two
        // complete updates (every update is one call here).
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut state)
    }
}

impl ExtractionLedger for MemoryExtractionLedger {
    async fn done(&self, exchange: ExchangeId) -> Result<bool, LedgerError> {
        Ok(self.with(|state| state.done.contains_key(&exchange)))
    }

    async fn context(
        &self,
        agent: AgentId,
        conversation: ConversationId,
    ) -> Result<Option<ConversationContext>, LedgerError> {
        Ok(self.with(|state| {
            state
                .contexts
                .get(&(agent, conversation))
                .map(|(context, _)| context.clone())
        }))
    }

    async fn pending(
        &self,
        agent: AgentId,
        call_id: &str,
    ) -> Result<Vec<PendingCall>, LedgerError> {
        Ok(self.with(|state| {
            state
                .pending
                .get(&(agent, call_id.to_owned()))
                .cloned()
                .unwrap_or_default()
        }))
    }

    async fn history(
        &self,
        conversation: ConversationId,
        call_id: &str,
    ) -> Result<Option<ToolCall>, LedgerError> {
        Ok(self.with(|state| {
            state
                .history
                .get(&(conversation, call_id.to_owned()))
                .cloned()
        }))
    }

    async fn delivered(&self, key: DeliveryKey) -> Result<bool, LedgerError> {
        Ok(self.with(|state| state.delivered.contains_key(&key)))
    }

    async fn commit(&self, commit: LedgerCommit) -> Result<(), LedgerError> {
        self.with(|state| state.apply(commit));
        Ok(())
    }

    async fn expire(&self, horizon: Timestamp) -> Result<(), LedgerError> {
        self.with(|state| state.expire(horizon));
        Ok(())
    }
}

/// A tool call as stored: the spec has no serde form for one, so it is the
/// canonical encoding of an assistant message holding just that call
/// (`encoding::encode`), read back only as exactly that.
pub(crate) mod stored_call {
    use crosstalk_spec::observed::message::{AssistantPart, MessageBody, ToolCall, encoding};
    use serde::{Deserialize, Deserializer, Serializer};

    /// The stored text of `call`.
    pub(crate) fn encode(call: &ToolCall) -> String {
        let body = MessageBody::Assistant(vec![AssistantPart::ToolCall(call.clone())]);
        // The encoding is canonical JSON text, so valid UTF-8.
        String::from_utf8_lossy(&encoding::encode(&body)).into_owned()
    }

    /// The call stored as `text`, or why it is not one.
    pub(crate) fn decode(text: &str) -> Result<ToolCall, String> {
        let body = encoding::decode(text.as_bytes()).map_err(|error| format!("{error:?}"))?;
        match body {
            MessageBody::Assistant(parts) => match <[AssistantPart; 1]>::try_from(parts) {
                Ok([AssistantPart::ToolCall(call)]) => Ok(call),
                _ => Err("not exactly one tool call".to_owned()),
            },
            _ => Err("not an assistant message".to_owned()),
        }
    }

    pub(super) fn serialize<S: Serializer>(
        call: &ToolCall,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(call))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<ToolCall, D::Error> {
        let text = String::deserialize(deserializer)?;
        decode(&text).map_err(serde::de::Error::custom)
    }
}
