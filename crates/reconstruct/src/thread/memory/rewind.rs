//! The in-memory store's side of order-tolerant threading
//! (`super::super::order`): finding the turns threaded after a late
//! exchange's place, undoing one, and threading one call, all under the
//! store's lock.

use std::collections::HashMap;
use std::ops::Bound;

use crosstalk_spec::ids::{AgentId, ConversationId, ExchangeId};
use crosstalk_spec::interfaces::l3_reconstruction::ThreadOutcome;

use super::super::config::SeenRetention;
use super::super::order::{LaterTurn, OrderedStore, ReorderWindow, ThreadCall, TurnKey};
use super::super::plan::{Planned, plan};
use super::super::store::ThreadInput;
use super::{Reads, State, StoredEntry};
use crate::error::{StorageFailure, TxFailure};

fn inconsistent(reason: String) -> TxFailure {
    TxFailure::Other(StorageFailure::Inconsistent { reason })
}

impl State {
    /// The turns of `members` taken at or after the first one whose key is
    /// greater than `key`, in the order they were taken.
    fn later(&self, members: &[AgentId], key: TurnKey) -> Vec<LaterTurn> {
        let first = self
            .by_key
            .range((Bound::Excluded(key), Bound::Unbounded))
            .filter_map(|later| self.turn_meta.get(&later.exchange))
            .filter(|meta| members.contains(&meta.agent))
            .map(|meta| meta.seq)
            .min();
        let Some(first) = first else {
            return Vec::new();
        };
        self.by_seq
            .range(first..)
            .filter_map(|(_, exchange)| {
                let meta = self.turn_meta.get(exchange)?;
                if !members.contains(&meta.agent) {
                    return None;
                }
                let (conversation, turn) = *self.turn_of.get(exchange)?;
                let row = self.turns.get(&conversation)?.get(turn as usize)?;
                Some(LaterTurn {
                    seq: meta.seq,
                    key: meta.key,
                    conversation,
                    turn,
                    first_ordinal: row.first_ordinal,
                })
            })
            .collect()
    }

    /// Remove what `turn`'s write added. It is its conversation's last
    /// turn.
    fn unthread(&mut self, turn: &LaterTurn) -> Result<(), TxFailure> {
        let exchange = turn.key.exchange;
        let conversation = turn.conversation;
        let rows = self.turns.get_mut(&conversation).ok_or_else(|| {
            inconsistent(format!(
                "conversation {} has no turns",
                conversation.ulid_text()
            ))
        })?;
        if rows.len() != turn.turn as usize + 1 {
            return Err(inconsistent(format!(
                "turn {} of {} is not its last",
                turn.turn,
                conversation.ulid_text()
            )));
        }
        rows.pop();
        self.records.remove(&exchange);
        self.turn_of.remove(&exchange);
        self.turn_meta.remove(&exchange);
        self.by_key.remove(&turn.key);
        self.by_seq.remove(&turn.seq);
        if let Some(key) = self.filed.remove(&exchange) {
            self.responses.remove(&key);
        }
        for sighting in self.seen_by_exchange.remove(&exchange).unwrap_or_default() {
            self.unsee(sighting, exchange);
        }
        let removed: Vec<StoredEntry> = match self.entries.get_mut(&conversation) {
            Some(entries) if turn.turn == 0 => std::mem::take(entries),
            Some(entries) => entries.split_off((turn.first_ordinal as usize).min(entries.len())),
            None => Vec::new(),
        };
        for entry in &removed {
            self.unindex(conversation, entry);
        }
        let old_head = self
            .conversations
            .get(&conversation)
            .and_then(|stored| stored.head);
        if let Some(head) = old_head
            && let Some(set) = self.heads.get_mut(&head)
        {
            set.remove(&conversation);
            if set.is_empty() {
                self.heads.remove(&head);
            }
        }
        if turn.turn == 0 {
            self.conversations.remove(&conversation);
            self.entries.remove(&conversation);
            self.turns.remove(&conversation);
            return Ok(());
        }
        self.restore(conversation)
    }

    /// Point `conversation` at its last turn again: its history length,
    /// head, last system message, last key and time.
    fn restore(&mut self, conversation: ConversationId) -> Result<(), TxFailure> {
        let missing = || {
            inconsistent(format!(
                "conversation {} lost its last turn",
                conversation.ulid_text()
            ))
        };
        let last = *self
            .turns
            .get(&conversation)
            .and_then(|rows| rows.last())
            .ok_or_else(missing)?;
        let meta = *self.turn_meta.get(&last.exchange).ok_or_else(missing)?;
        let head = match last.history_end.checked_sub(1) {
            None => None,
            Some(index) => self
                .entries
                .get(&conversation)
                .and_then(|entries| {
                    entries.iter().find_map(|entry| {
                        entry
                            .history
                            .filter(|(at, _)| *at == index)
                            .map(|(_, chain)| chain)
                    })
                })
                .map(Some)
                .ok_or_else(missing)?,
        };
        let stored = self
            .conversations
            .get_mut(&conversation)
            .ok_or_else(missing)?;
        stored.history_len = last.history_end;
        stored.head = head;
        stored.last_system = meta.last_system;
        stored.last_key = meta.key;
        stored.last_turn_at = last.started_at;
        if let Some(head) = head {
            self.heads.entry(head).or_default().insert(conversation);
        }
        Ok(())
    }
}

/// One threading call's step: the locked state and the store's settings.
pub(super) struct Step<'a> {
    pub(super) state: &'a mut State,
    pub(super) retention: SeenRetention,
    pub(super) window: ReorderWindow,
}

impl OrderedStore for Step<'_> {
    async fn recorded(&mut self, exchange: ExchangeId) -> Result<Option<ThreadOutcome>, TxFailure> {
        Ok(self.state.records.get(&exchange).cloned())
    }

    async fn later(
        &mut self,
        members: &[AgentId],
        key: TurnKey,
    ) -> Result<Vec<LaterTurn>, TxFailure> {
        Ok(self.state.later(members, key))
    }

    async fn turn_counts(
        &mut self,
        conversations: &[ConversationId],
    ) -> Result<HashMap<ConversationId, u32>, TxFailure> {
        Ok(conversations
            .iter()
            .map(|id| {
                let count = self.state.turns.get(id).map_or(0, Vec::len);
                (*id, u32::try_from(count).unwrap_or(u32::MAX))
            })
            .collect())
    }

    async fn calls(
        &mut self,
        exchanges: &[ExchangeId],
    ) -> Result<HashMap<ExchangeId, ThreadCall>, TxFailure> {
        Ok(exchanges
            .iter()
            .filter_map(|id| self.state.calls.get(id).map(|call| (*id, call.clone())))
            .collect())
    }

    async fn unthread(&mut self, turn: &LaterTurn) -> Result<(), TxFailure> {
        self.state.unthread(turn)
    }

    async fn thread_now(&mut self, input: &ThreadInput) -> Result<ThreadOutcome, TxFailure> {
        let planned = {
            let mut reads = Reads {
                state: self.state,
                cutoff: self.retention.cutoff(input.at),
            };
            plan(&mut reads, input).await?
        };
        match planned {
            Planned::Recorded(outcome) => Ok(outcome),
            Planned::Write(write) => {
                self.state
                    .apply(input, &write, self.retention, self.window)
                    .map_err(TxFailure::Other)?;
                Ok(write.outcome)
            }
        }
    }
}
