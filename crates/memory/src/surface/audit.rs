//! [`InMemoryAuditLog`]: the reference [`AuditLog`]. Append-only: nothing
//! here updates or removes an entry.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crosstalk_spec::ids::AuditId;
use crosstalk_spec::interfaces::l8_surface::audit::{
    AuditEntry, AuditError, AuditFilter, AuditLog,
};
use crosstalk_spec::paging::{AuditList, Page, PageRequest};
use crosstalk_spec::support::Timestamp;

use super::paging::{CursorBook, page_after};
use crate::analysis::support::lock;

/// The reference audit log. Cloning shares the log.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAuditLog {
    state: Arc<Mutex<AuditState>>,
}

#[derive(Debug, Default)]
struct AuditState {
    entries: BTreeMap<AuditId, AuditEntry>,
    cursors: CursorBook<AuditFilter, (Timestamp, AuditId)>,
}

impl AuditState {
    /// Whether `entry` can be appended: a new id, or exactly the entry
    /// already stored under it.
    fn check(&self, entry: &AuditEntry) -> Result<(), AuditError> {
        match self.entries.get(&entry.id) {
            Some(stored) if stored != entry => Err(AuditError::IdReused(entry.id)),
            Some(_) | None => Ok(()),
        }
    }
}

impl InMemoryAuditLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append every entry or none, as one transaction: the config changes of
    /// one load, recorded with the change they describe.
    pub fn append_all(&self, entries: &[AuditEntry]) -> Result<(), AuditError> {
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

    /// Every entry, by id.
    pub fn entries(&self) -> Vec<AuditEntry> {
        lock(&self.state).entries.values().cloned().collect()
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
