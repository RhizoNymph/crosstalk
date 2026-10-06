//! [`InMemoryAuditLog`]: the reference [`AuditLog`] and [`AuditIntents`].
//! Append-only: nothing here updates or removes an entry.
//!
//! Intents (the write-ahead half, `surface.audit.no-silent-effect`) are
//! kept beside the entries:
//!
//! ```text
//! intend(intent)      ─▶ intents[id] = intent   (same intent again: no-op; id held by an entry or another intent: IdReused)
//! complete(entry)     ─▶ entries[id] = entry, intents[id] removed, together
//!                        (entry must be the intent's call: same at, caller, action; else IdReused)
//! append(entry)       ─▶ IdReused while an intent holds the id (the intent reserves it)
//! recover_interrupted ─▶ each intent, oldest (at, id) first: entries[id] = intent.interrupted(), removed
//! ```

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crosstalk_spec::ids::AuditId;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditBody, AuditEntry, AuditError, AuditFilter, AuditIntent, AuditIntents, AuditLog,
};
use crosstalk_spec::paging::{AuditList, Page, PageRequest};
use crosstalk_spec::support::Timestamp;

use crate::support::{CursorBook, lock, page_after};

/// The reference audit log. Cloning shares the log.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAuditLog {
    state: Arc<Mutex<AuditState>>,
}

#[derive(Debug, Default)]
struct AuditState {
    entries: BTreeMap<AuditId, AuditEntry>,
    /// Calls whose effect may be applying: recorded before it, removed
    /// with the call's entry.
    intents: BTreeMap<AuditId, AuditIntent>,
    cursors: CursorBook<AuditFilter, (Timestamp, AuditId)>,
}

impl AuditState {
    /// Whether `entry` can be appended: a new id no intent holds, or
    /// exactly the entry already stored under it.
    fn check(&self, entry: &AuditEntry) -> Result<(), AuditError> {
        match self.entries.get(&entry.id) {
            Some(stored) if stored != entry => Err(AuditError::IdReused(entry.id)),
            Some(_) => Ok(()),
            None if self.intents.contains_key(&entry.id) => Err(AuditError::IdReused(entry.id)),
            None => Ok(()),
        }
    }
}

/// Whether `entry` records the call `intent` announced: an operator entry
/// with the intent's id, time, caller and action.
fn completes(intent: &AuditIntent, entry: &AuditEntry) -> bool {
    match &entry.body {
        AuditBody::Operator(record) => {
            entry.id == intent.id()
                && entry.at == intent.at()
                && record.caller() == intent.caller()
                && record.action() == intent.action()
        }
        AuditBody::Config(_) | AuditBody::Export(_) => false,
    }
}

impl InMemoryAuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append every entry or none, as one transaction: the config changes of
    /// one load, recorded with the change they describe.
    pub(crate) fn append_all(&self, entries: &[AuditEntry]) -> Result<(), AuditError> {
        let mut state = lock(&self.state);
        for entry in entries {
            state.check(entry)?;
        }
        for entry in entries {
            state
                .entries
                .entry(entry.id)
                .or_insert_with(|| entry.clone());
        }
        Ok(())
    }

    /// Every entry, by id, for tests that check what a load recorded.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> Vec<AuditEntry> {
        lock(&self.state).entries.values().cloned().collect()
    }
}

impl AuditIntents for InMemoryAuditLog {
    async fn intend(&mut self, intent: &AuditIntent) -> Result<(), AuditError> {
        let mut state = lock(&self.state);
        if state.entries.contains_key(&intent.id()) {
            return Err(AuditError::IdReused(intent.id()));
        }
        match state.intents.get(&intent.id()) {
            Some(held) if held == intent => Ok(()),
            Some(_) => Err(AuditError::IdReused(intent.id())),
            None => {
                state.intents.insert(intent.id(), intent.clone());
                Ok(())
            }
        }
    }

    async fn complete(&mut self, entry: AuditEntry) -> Result<(), AuditError> {
        let mut state = lock(&self.state);
        let Some(intent) = state.intents.get(&entry.id) else {
            state.check(&entry)?;
            state.entries.entry(entry.id).or_insert(entry);
            return Ok(());
        };
        if !completes(intent, &entry) {
            return Err(AuditError::IdReused(entry.id));
        }
        state.intents.remove(&entry.id);
        state.entries.entry(entry.id).or_insert(entry);
        Ok(())
    }

    async fn recover_interrupted(&mut self) -> Result<Vec<AuditId>, AuditError> {
        let mut state = lock(&self.state);
        let mut leftover: Vec<AuditIntent> =
            std::mem::take(&mut state.intents).into_values().collect();
        leftover.sort_by_key(|intent| (intent.at(), intent.id()));
        let mut recovered = Vec::with_capacity(leftover.len());
        for intent in leftover {
            state
                .entries
                .entry(intent.id())
                .or_insert_with(|| intent.interrupted());
            recovered.push(intent.id());
        }
        Ok(recovered)
    }
}

impl AuditLog for InMemoryAuditLog {
    async fn append(&mut self, entry: AuditEntry) -> Result<(), AuditError> {
        self.append_all(std::slice::from_ref(&entry))
    }

    async fn query(
        &self,
        filter: &AuditFilter,
        page: &PageRequest<AuditList>,
    ) -> Result<Page<AuditEntry, AuditList>, AuditError> {
        let mut state = lock(&self.state);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                state
                    .cursors
                    .resolve(cursor, filter)
                    .ok_or(AuditError::InvalidCursor)?,
            ),
        };
        let mut remaining: Vec<AuditEntry> = state
            .entries
            .values()
            .filter(|entry| filter.matches(entry))
            .filter(|entry| after.is_none_or(|after| (entry.at, entry.id) < after))
            .cloned()
            .collect();
        remaining.sort_by_key(|entry| std::cmp::Reverse((entry.at, entry.id)));
        page_after(
            &mut state.cursors,
            remaining,
            page.size,
            filter.clone(),
            |entry| (entry.at, entry.id),
        )
        .map_err(|error| AuditError::Store {
            reason: error.to_string(),
        })
    }
}
