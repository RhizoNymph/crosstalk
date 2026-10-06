//! One delta's view of the ledger: every read goes through it, every
//! change stays in it until the step commits it
//! ([`Working::into_changes`]). A read sees the delta's own earlier
//! changes, exactly as the gateway's in-memory maps did, and nothing in
//! the ledger changes when the delta fails or its inputs are not durable.

use std::collections::{BTreeMap, BTreeSet};

use crosstalk_spec::ids::{AgentId, ConversationId};
use crosstalk_spec::observed::message::ToolCall;

use super::ledger::{
    ContextKey, DeliveryKey, ExtractionLedger, HistoryKey, LedgerChanges, LedgerError, PendingCall,
    PendingKey,
};
use crate::extract::ConversationContext;

pub(crate) struct Working<'l, L> {
    ledger: &'l L,
    /// Contexts read or set, and which of them were set.
    contexts: BTreeMap<ContextKey, Option<ConversationContext>>,
    set_contexts: BTreeSet<ContextKey>,
    /// Pending lists read or changed, and which of them changed.
    pending: BTreeMap<PendingKey, Vec<PendingCall>>,
    changed_pending: BTreeSet<PendingKey>,
    /// History calls read, and which were stored or taken.
    history: BTreeMap<HistoryKey, Option<ToolCall>>,
    changed_history: BTreeSet<HistoryKey>,
    delivered: BTreeSet<DeliveryKey>,
}

impl<'l, L: ExtractionLedger> Working<'l, L> {
    pub(crate) fn new(ledger: &'l L) -> Self {
        Self {
            ledger,
            contexts: BTreeMap::new(),
            set_contexts: BTreeSet::new(),
            pending: BTreeMap::new(),
            changed_pending: BTreeSet::new(),
            history: BTreeMap::new(),
            changed_history: BTreeSet::new(),
            delivered: BTreeSet::new(),
        }
    }

    /// `agent`'s context in `conversation`, if it has one.
    pub(crate) async fn context(
        &mut self,
        agent: AgentId,
        conversation: ConversationId,
    ) -> Result<Option<ConversationContext>, LedgerError> {
        let key = (agent, conversation);
        if let Some(context) = self.contexts.get(&key) {
            return Ok(context.clone());
        }
        let stored = self.ledger.context(agent, conversation).await?;
        self.contexts.insert(key, stored.clone());
        Ok(stored)
    }

    pub(crate) fn set_context(&mut self, key: ContextKey, context: ConversationContext) {
        self.contexts.insert(key, Some(context));
        self.set_contexts.insert(key);
    }

    /// The calls `agent` made with `call_id`, waiting for a result, for
    /// changing in place ([`Working::pending_changed`] records a change).
    pub(crate) async fn pending(
        &mut self,
        agent: AgentId,
        call_id: &str,
    ) -> Result<&mut Vec<PendingCall>, LedgerError> {
        let key = (agent, call_id.to_owned());
        if !self.pending.contains_key(&key) {
            let stored = self.ledger.pending(agent, call_id).await?;
            self.pending.insert(key.clone(), stored);
        }
        Ok(self.pending.entry(key).or_default())
    }

    pub(crate) fn pending_changed(&mut self, agent: AgentId, call_id: &str) {
        self.changed_pending.insert((agent, call_id.to_owned()));
    }

    pub(crate) async fn history(
        &mut self,
        conversation: ConversationId,
        call_id: &str,
    ) -> Result<Option<ToolCall>, LedgerError> {
        let key = (conversation, call_id.to_owned());
        if let Some(call) = self.history.get(&key) {
            return Ok(call.clone());
        }
        let stored = self.ledger.history(conversation, call_id).await?;
        self.history.insert(key, stored.clone());
        Ok(stored)
    }

    pub(crate) fn set_history(&mut self, key: HistoryKey, call: Option<ToolCall>) {
        self.history.insert(key.clone(), call);
        self.changed_history.insert(key);
    }

    /// Whether `key` was delivered, before this delta or in it.
    pub(crate) async fn delivered(&self, key: DeliveryKey) -> Result<bool, LedgerError> {
        if self.delivered.contains(&key) {
            return Ok(true);
        }
        self.ledger.delivered(key).await
    }

    pub(crate) fn deliver(&mut self, key: DeliveryKey) {
        self.delivered.insert(key);
    }

    /// What the delta changed.
    pub(crate) fn into_changes(self) -> LedgerChanges {
        let Self {
            contexts,
            set_contexts,
            pending,
            changed_pending,
            history,
            changed_history,
            delivered,
            ..
        } = self;
        LedgerChanges {
            contexts: contexts
                .into_iter()
                .filter(|(key, _)| set_contexts.contains(key))
                .filter_map(|(key, context)| context.map(|context| (key, context)))
                .collect(),
            pending: pending
                .into_iter()
                .filter(|(key, _)| changed_pending.contains(key))
                .collect(),
            history: history
                .into_iter()
                .filter(|(key, _)| changed_history.contains(key))
                .collect(),
            delivered,
        }
    }
}
