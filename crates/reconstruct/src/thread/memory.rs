//! [`MemoryConversations`]: the conversation store in memory, for a
//! single-process gateway without a database, the simulation, and as the
//! reference the Postgres store is model-tested against.
//!
//! One `tokio::sync::Mutex` guards the whole state; a threading call takes
//! it once, decides (`plan`) and applies the write before
//! releasing it, so calls are serializable.
//!
//! The seen-message set (`reconstruct.delta.excludes-seen-elsewhere`)
//! keeps, per attributed agent and message, the latest time it was seen in
//! each conversation; sightings older than the retention behind the latest
//! exchange threaded are forgotten after each write, so the set stays
//! bounded by the retention.
//!
//! Each write also records its turn (the spec's `ConversationReads`, read
//! in its `reads` submodule).

mod reads;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l3_reconstruction::{
    ExchangePlacements, Placement, ThreadError, ThreadOutcome,
};
use crosstalk_spec::observed::client::TrafficSource;
use crosstalk_spec::observed::conversation::{Conversation, ConversationOrigin};
use crosstalk_spec::support::Timestamp;
use tokio::sync::Mutex;

use super::config::{SeenRetention, ThreadConfig};

use super::history::{ChainHash, Entry};
use super::plan::{Extension, Planned, Target, ThreadReads, Write, plan};
use super::reads::{Cursors, TurnRow};
use super::store::{ConversationStore, ResponseKey, ThreadInput, TranscriptEntry};
use crate::error::{StorageFailure, StoreReason, TxFailure};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Stored {
    agent: AgentId,
    origin: ConversationOrigin,
    history_len: u32,
    head: Option<ChainHash>,
    last_system: Option<MessageHash>,
    updated: u64,
    source: TrafficSource,
    started_at: Timestamp,
    last_turn_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StoredEntry {
    entry: Entry,
    exchange: ExchangeId,
    history: Option<(u32, ChainHash)>,
    output: bool,
    carried_over: bool,
}

impl StoredEntry {
    /// The entry at `ordinal`.
    fn transcript(&self, ordinal: u32) -> TranscriptEntry {
        TranscriptEntry {
            ordinal,
            message: self.entry.message,
            role: self.entry.role,
            exchange: self.exchange,
            history_index: self.history.map(|(index, _)| index),
            output: self.output,
            carried_over: self.carried_over,
        }
    }
}

/// Every conversation and the indexes the decision looks up.
#[derive(Debug, Default)]
struct State {
    conversations: BTreeMap<ConversationId, Stored>,
    entries: BTreeMap<ConversationId, Vec<StoredEntry>>,
    heads: HashMap<ChainHash, BTreeSet<ConversationId>>,
    chains: HashMap<ChainHash, BTreeSet<ConversationId>>,
    holders: HashMap<MessageHash, BTreeSet<ConversationId>>,
    outputs: HashMap<MessageHash, BTreeSet<ConversationId>>,
    records: HashMap<ExchangeId, ThreadOutcome>,
    /// Each conversation's turns, in threading order.
    turns: HashMap<ConversationId, Vec<TurnRow>>,
    /// Where each threaded exchange is: its conversation and turn.
    turn_of: HashMap<ExchangeId, (ConversationId, u32)>,
    responses: HashMap<ResponseKey, (ConversationId, u32)>,
    updates: u64,
    /// Each attributed agent's sightings of a message: the latest time it
    /// was seen in each conversation.
    seen: HashMap<(AgentId, MessageHash), HashMap<ConversationId, Timestamp>>,
    /// The same sightings ordered by time, to forget the oldest.
    seen_by_time: BTreeSet<(Timestamp, AgentId, MessageHash, ConversationId)>,
    /// The latest exchange start threaded.
    horizon: Option<Timestamp>,
}

impl State {
    /// The most recently threaded of `candidates` that belongs to
    /// `members`.
    fn latest_of<'a>(
        &self,
        members: &[AgentId],
        candidates: impl IntoIterator<Item = &'a ConversationId>,
    ) -> Option<ConversationId> {
        candidates
            .into_iter()
            .filter_map(|id| self.conversations.get(id).map(|stored| (id, stored)))
            .filter(|(_, stored)| members.contains(&stored.agent))
            .max_by_key(|(_, stored)| stored.updated)
            .map(|(id, _)| *id)
    }

    fn index(&mut self, conversation: ConversationId, entry: &StoredEntry) {
        if let Some((_, chain)) = entry.history {
            self.chains.entry(chain).or_default().insert(conversation);
            self.holders
                .entry(entry.entry.message)
                .or_default()
                .insert(conversation);
        }
        if entry.output {
            self.outputs
                .entry(entry.entry.message)
                .or_default()
                .insert(conversation);
        }
    }

    /// Record that `agent` saw `messages` in `conversation` at `at`, then
    /// forget every sighting older than `retention` behind the latest
    /// exchange threaded.
    fn see(
        &mut self,
        agent: AgentId,
        conversation: ConversationId,
        messages: &[MessageHash],
        at: Timestamp,
        retention: SeenRetention,
    ) {
        for message in messages {
            let sightings = self.seen.entry((agent, *message)).or_default();
            let previous = sightings.get(&conversation).copied();
            if previous.is_some_and(|previous| previous >= at) {
                continue;
            }
            sightings.insert(conversation, at);
            if let Some(previous) = previous {
                self.seen_by_time
                    .remove(&(previous, agent, *message, conversation));
            }
            self.seen_by_time
                .insert((at, agent, *message, conversation));
        }
        let horizon = self.horizon.map_or(at, |horizon| horizon.max(at));
        self.horizon = Some(horizon);
        let cutoff = retention.cutoff(horizon);
        while let Some(&(time, agent, message, conversation)) = self.seen_by_time.first() {
            if time >= cutoff {
                break;
            }
            self.seen_by_time.pop_first();
            if let Some(sightings) = self.seen.get_mut(&(agent, message)) {
                sightings.remove(&conversation);
                if sightings.is_empty() {
                    self.seen.remove(&(agent, message));
                }
            }
        }
    }

    fn apply(
        &mut self,
        input: &ThreadInput,
        write: &Write,
        retention: SeenRetention,
    ) -> Result<(), StorageFailure> {
        let exchange = input.exchange;
        self.updates += 1;
        let id = write.conversation;
        match write.target {
            Target::New {
                agent,
                origin,
                base,
            } => {
                let mut entries: Vec<StoredEntry> = Vec::new();
                if let Some((parent, k)) = base {
                    let parent_entries =
                        self.entries
                            .get(&parent)
                            .ok_or_else(|| StorageFailure::Inconsistent {
                                reason: format!("fork parent {} is not stored", parent.ulid_text()),
                            })?;
                    for stored in parent_entries {
                        // A fork's base belongs to no turn of the fork.
                        entries.push(StoredEntry {
                            carried_over: false,
                            ..*stored
                        });
                        if stored.history.is_some_and(|(index, _)| index + 1 == k) {
                            break;
                        }
                    }
                }
                self.conversations.insert(
                    id,
                    Stored {
                        agent,
                        origin,
                        history_len: 0,
                        head: None,
                        last_system: None,
                        updated: 0,
                        source: input.source.clone(),
                        started_at: input.at,
                        last_turn_at: input.at,
                    },
                );
                for stored in &entries {
                    self.index(id, stored);
                }
                self.entries.insert(id, entries);
            }
            Target::Existing => {}
        }
        let appended: Vec<StoredEntry> = write
            .appended
            .iter()
            .map(|new| StoredEntry {
                entry: new.entry,
                exchange,
                history: new.history,
                output: new.output,
                carried_over: new.carried_over,
            })
            .collect();
        for stored in &appended {
            self.index(id, stored);
        }
        let transcript = self.entries.entry(id).or_default();
        let first_ordinal = u32::try_from(transcript.len()).map_err(|_| too_long(id))?;
        let count = u32::try_from(appended.len()).map_err(|_| too_long(id))?;
        transcript.extend(appended);
        let turns = self.turns.entry(id).or_default();
        let turn = u32::try_from(turns.len()).map_err(|_| too_long(id))?;
        turns.push(TurnRow {
            exchange,
            first_ordinal,
            entries: count,
            agent: input.agent,
            started_at: input.at,
            outcome: write.outcome.kind(),
            history_end: write.history_len,
        });
        self.turn_of.insert(exchange, (id, turn));
        let updates = self.updates;
        let stored =
            self.conversations
                .get_mut(&id)
                .ok_or_else(|| StorageFailure::Inconsistent {
                    reason: format!("conversation {} is not stored", id.ulid_text()),
                })?;
        let old_head = stored.head;
        stored.history_len = write.history_len;
        stored.head = write.head;
        stored.last_system = write.last_system;
        stored.updated = updates;
        stored.last_turn_at = input.at;
        if let Some(old) = old_head
            && let Some(set) = self.heads.get_mut(&old)
        {
            set.remove(&id);
        }
        if let Some(head) = write.head {
            self.heads.entry(head).or_default().insert(id);
        }
        self.records.insert(exchange, write.outcome.clone());
        if let Some((key, len)) = &write.response {
            self.responses.entry(key.clone()).or_insert((id, *len));
        }
        self.see(input.agent, id, &write.seen, input.at, retention);
        Ok(())
    }
}

fn too_long(conversation: ConversationId) -> StorageFailure {
    StorageFailure::Inconsistent {
        reason: format!(
            "conversation {} is beyond the stored range",
            conversation.ulid_text()
        ),
    }
}

/// Reads of the state, ready at once; sightings before `cutoff` do not
/// count.
struct Reads<'a> {
    state: &'a State,
    cutoff: Timestamp,
}

impl ThreadReads for Reads<'_> {
    async fn recorded(&mut self, exchange: ExchangeId) -> Result<Option<ThreadOutcome>, TxFailure> {
        Ok(self.state.records.get(&exchange).cloned())
    }

    async fn extension(
        &mut self,
        members: &[AgentId],
        chains: &[ChainHash],
    ) -> Result<Option<Extension>, TxFailure> {
        for (index, chain) in chains.iter().enumerate().rev() {
            let len = (index + 1) as u32;
            let Some(candidates) = self.state.heads.get(chain) else {
                continue;
            };
            let matching: Vec<&ConversationId> = candidates
                .iter()
                .filter(|id| {
                    self.state
                        .conversations
                        .get(id)
                        .is_some_and(|stored| stored.history_len == len)
                })
                .collect();
            if let Some(conversation) = self.state.latest_of(members, matching) {
                let last_system = self
                    .state
                    .conversations
                    .get(&conversation)
                    .and_then(|stored| stored.last_system);
                return Ok(Some(Extension {
                    conversation,
                    len,
                    last_system,
                }));
            }
        }
        Ok(None)
    }

    async fn common_prefix(
        &mut self,
        members: &[AgentId],
        chains: &[ChainHash],
        from: usize,
    ) -> Result<Option<(ConversationId, u32)>, TxFailure> {
        for (index, chain) in chains.iter().enumerate().rev() {
            if index < from {
                break;
            }
            if let Some(candidates) = self.state.chains.get(chain)
                && let Some(conversation) = self.state.latest_of(members, candidates)
            {
                return Ok(Some((conversation, (index + 1) as u32)));
            }
        }
        Ok(None)
    }

    async fn response(
        &mut self,
        key: &ResponseKey,
        members: &[AgentId],
    ) -> Result<Option<(ConversationId, u32)>, TxFailure> {
        Ok(self
            .state
            .responses
            .get(key)
            .copied()
            .filter(|(conversation, _)| {
                self.state
                    .conversations
                    .get(conversation)
                    .is_some_and(|stored| members.contains(&stored.agent))
            }))
    }

    async fn history(
        &mut self,
        conversation: ConversationId,
        len: u32,
    ) -> Result<Vec<Entry>, TxFailure> {
        Ok(self
            .state
            .entries
            .get(&conversation)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|stored| stored.history.is_some_and(|(index, _)| index < len))
                    .map(|stored| stored.entry)
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn output_holder(
        &mut self,
        members: &[AgentId],
        message: MessageHash,
    ) -> Result<Option<ConversationId>, TxFailure> {
        Ok(self
            .state
            .outputs
            .get(&message)
            .and_then(|candidates| self.state.latest_of(members, candidates)))
    }

    async fn holder(
        &mut self,
        members: &[AgentId],
        messages: &[MessageHash],
    ) -> Result<Option<ConversationId>, TxFailure> {
        let candidates: BTreeSet<ConversationId> = messages
            .iter()
            .filter_map(|message| self.state.holders.get(message))
            .flatten()
            .copied()
            .collect();
        Ok(self.state.latest_of(members, &candidates))
    }

    async fn latest(&mut self, members: &[AgentId]) -> Result<Option<ConversationId>, TxFailure> {
        Ok(self
            .state
            .latest_of(members, self.state.conversations.keys()))
    }

    async fn seen_elsewhere(
        &mut self,
        members: &[AgentId],
        messages: &[MessageHash],
        conversation: ConversationId,
    ) -> Result<HashSet<MessageHash>, TxFailure> {
        Ok(messages
            .iter()
            .copied()
            .filter(|message| {
                members.iter().any(|agent| {
                    self.state
                        .seen
                        .get(&(*agent, *message))
                        .is_some_and(|sightings| {
                            sightings
                                .iter()
                                .any(|(other, at)| *other != conversation && *at >= self.cutoff)
                        })
                })
            })
            .collect())
    }

    async fn held(
        &mut self,
        conversation: ConversationId,
        messages: &[MessageHash],
    ) -> Result<HashSet<MessageHash>, TxFailure> {
        let wanted: HashSet<&MessageHash> = messages.iter().collect();
        Ok(self
            .state
            .entries
            .get(&conversation)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|stored| stored.history.is_some())
                    .map(|stored| stored.entry.message)
                    .filter(|message| wanted.contains(message))
                    .collect()
            })
            .unwrap_or_default())
    }
}

/// The in-memory conversation store. Clones are handles on one store.
#[derive(Debug, Clone, Default)]
pub struct MemoryConversations {
    state: Arc<Mutex<State>>,
    config: ThreadConfig,
    cursors: Cursors,
}

impl MemoryConversations {
    /// An empty store with the default [`ThreadConfig`].
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store with `config`.
    pub fn with_config(config: ThreadConfig) -> Self {
        Self {
            state: Arc::default(),
            config,
            cursors: Cursors::default(),
        }
    }

    /// The same store, its list cursors tagged with `key`.
    pub fn with_cursor_key(mut self, key: [u8; 32]) -> Self {
        self.cursors = Cursors { key };
        self
    }

    /// The same store, its list cursors keyed from the deployment `secret`
    /// ([`crate::ids::CONVERSATIONS_CURSOR_LABEL`]), as
    /// `PgConversations::with_cursor_secret` keys them.
    pub fn with_cursor_secret(self, secret: &crosstalk_spec::ids::KeyedHasher) -> Self {
        self.with_cursor_key(crate::ids::cursor_key(
            secret,
            crate::ids::CONVERSATIONS_CURSOR_LABEL,
        ))
    }
}

impl ExchangePlacements for MemoryConversations {
    async fn placement(&self, exchange: ExchangeId) -> Result<Option<Placement>, ThreadError> {
        let state = self.state.lock().await;
        Ok(state.records.get(&exchange).map(Placement::of))
    }
}

impl ConversationStore for MemoryConversations {
    async fn thread(&self, input: ThreadInput) -> Result<ThreadOutcome, ThreadError> {
        let mut state = self.state.lock().await;
        let retention = self.config.seen_retention;
        let mut reads = Reads {
            state: &state,
            cutoff: retention.cutoff(input.at),
        };
        let planned = plan(&mut reads, &input)
            .await
            .map_err(|failure| ThreadError::from_failure(&failure.into()))?;
        match planned {
            Planned::Recorded(outcome) => Ok(outcome),
            Planned::Write(write) => {
                state
                    .apply(&input, &write, retention)
                    .map_err(|failure| ThreadError::from_failure(&failure))?;
                Ok(write.outcome)
            }
        }
    }

    async fn conversation(&self, id: ConversationId) -> Result<Option<Conversation>, ThreadError> {
        let state = self.state.lock().await;
        Ok(state.conversations.get(&id).map(|stored| Conversation {
            id,
            agent: stored.agent,
            messages: state
                .entries
                .get(&id)
                .map(|entries| {
                    entries
                        .iter()
                        .filter(|entry| entry.history.is_some())
                        .map(|entry| entry.entry.message)
                        .collect()
                })
                .unwrap_or_default(),
            origin: stored.origin,
        }))
    }

    async fn transcript(&self, id: ConversationId) -> Result<Vec<TranscriptEntry>, ThreadError> {
        let state = self.state.lock().await;
        Ok(state
            .entries
            .get(&id)
            .map(|entries| {
                entries
                    .iter()
                    .enumerate()
                    .map(|(ordinal, stored)| stored.transcript(ordinal as u32))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn conversations(&self) -> Result<Vec<ConversationId>, ThreadError> {
        let state = self.state.lock().await;
        Ok(state.conversations.keys().copied().collect())
    }
}
