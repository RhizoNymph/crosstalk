//! The spec's `ConversationReads` over [`MemoryConversations`]: every
//! read takes the store's lock once, so it reads one snapshot.

use crosstalk_spec::interfaces::l3_reconstruction::conversations::ExchangePlacement;
use std::collections::BTreeMap;

use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::ids::{ConversationId, ExchangeId};
use crosstalk_spec::interfaces::l3_reconstruction::conversations::{
    ConversationQuery, ConversationReadError, ConversationReads, StoredConversation, TurnIndex,
    TurnSlice, TurnWindow,
};
use crosstalk_spec::observed::conversation::Conversation;
use crosstalk_spec::paging::{ConversationList, Page, PageRequest};

use super::super::reads::binding;
use super::{MemoryConversations, State};

impl State {
    /// The conversation stored under `id`, as the list returns it.
    fn stored(&self, id: ConversationId) -> Option<StoredConversation> {
        let stored = self.conversations.get(&id)?;
        let messages = self
            .entries
            .get(&id)
            .map(|entries| {
                entries
                    .iter()
                    .filter(|entry| entry.history.is_some())
                    .map(|entry| entry.entry.message)
                    .collect()
            })
            .unwrap_or_default();
        let turns = self.turns.get(&id).map_or(0, Vec::len);
        Some(StoredConversation {
            conversation: Conversation {
                id,
                agent: stored.agent,
                messages,
                origin: stored.origin,
            },
            source: stored.source.clone(),
            started_at: stored.started_at,
            last_turn_at: stored.last_turn_at,
            turns: u32::try_from(turns).unwrap_or(u32::MAX),
        })
    }
}

impl ConversationReads for MemoryConversations {
    async fn list(
        &self,
        query: &ConversationQuery,
        page: &PageRequest<ConversationList>,
    ) -> Result<Page<StoredConversation, ConversationList>, ConversationReadError> {
        let binding = binding(query);
        let after = page
            .after
            .as_ref()
            .map(|cursor| self.cursors.resume(cursor, &binding))
            .transpose()?;
        let state = self.state.lock().await;
        let wanted = usize::from(page.size.get().get()) + 1;
        let rows: Vec<StoredConversation> = state
            .conversations
            .keys()
            .rev()
            .filter(|id| after.is_none_or(|after| **id < after))
            .filter_map(|id| state.stored(*id))
            .filter(|stored| query.admits(stored))
            .take(wanted)
            .collect();
        self.cursors.page(&binding, page.size, rows)
    }

    async fn conversation(
        &self,
        id: ConversationId,
    ) -> Result<Option<StoredConversation>, ConversationReadError> {
        Ok(self.state.lock().await.stored(id))
    }

    async fn successors(
        &self,
        id: ConversationId,
    ) -> Result<Vec<StoredConversation>, ConversationReadError> {
        let state = self.state.lock().await;
        let mut successors: Vec<StoredConversation> = state
            .conversations
            .iter()
            .filter(|(_, stored)| stored.origin.source() == Some(id))
            .filter_map(|(successor, _)| state.stored(*successor))
            .collect();
        successors.sort_by_key(|stored| (stored.started_at, stored.conversation.id));
        Ok(successors)
    }

    async fn turns(
        &self,
        id: ConversationId,
        window: &TurnWindow,
    ) -> Result<Option<TurnSlice>, ConversationReadError> {
        let state = self.state.lock().await;
        if !state.conversations.contains_key(&id) {
            return Ok(None);
        }
        let rows = state.turns.get(&id).map(Vec::as_slice).unwrap_or(&[]);
        let entries = state.entries.get(&id).map(Vec::as_slice).unwrap_or(&[]);
        let total = u32::try_from(rows.len()).unwrap_or(u32::MAX);
        let mut turns = Vec::new();
        for index in window.range(total) {
            let Some(row) = rows.get(index as usize) else {
                break;
            };
            let mut read = Vec::with_capacity(row.entries as usize);
            for ordinal in row.ordinals() {
                let entry =
                    entries
                        .get(ordinal as usize)
                        .ok_or_else(|| ConversationReadError::Store {
                            reason: format!(
                                "turn {index} of {} names ordinal {ordinal} past its transcript",
                                id.ulid_text()
                            ),
                        })?;
                read.push(entry.transcript(ordinal));
            }
            turns.push(row.turn(index, read));
        }
        Ok(Some(TurnSlice { total, turns }))
    }

    async fn locate(
        &self,
        ids: &IdBatch<ExchangeId>,
    ) -> Result<BTreeMap<ExchangeId, ExchangePlacement>, ConversationReadError> {
        let state = self.state.lock().await;
        Ok(ids
            .ids()
            .iter()
            .filter_map(|id| {
                let (conversation, turn) = state.turn_of.get(id)?;
                let row = state.turns.get(conversation)?.get(*turn as usize)?;
                Some((
                    *id,
                    ExchangePlacement {
                        agent: row.agent,
                        conversation: *conversation,
                        turn: TurnIndex(*turn),
                    },
                ))
            })
            .collect())
    }

    async fn branch_turn(
        &self,
        parent: ConversationId,
        shared_prefix: u32,
    ) -> Result<Option<TurnIndex>, ConversationReadError> {
        let state = self.state.lock().await;
        let rows = state.turns.get(&parent).map(Vec::as_slice).unwrap_or(&[]);
        Ok(rows
            .iter()
            .enumerate()
            .filter(|(_, row)| row.history_end <= shared_prefix)
            .map(|(index, _)| index)
            .next_back()
            .and_then(|index| u32::try_from(index).ok())
            .map(TurnIndex))
    }
}
