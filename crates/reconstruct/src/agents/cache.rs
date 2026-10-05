//! The merge table's in-process copy, which `AgentDirectory::canonical`
//! reads: the spec makes `canonical` synchronous, so the Postgres store
//! answers it from memory.
//!
//! The copy is loaded when the store opens and updated by every merge and
//! unmerge this store commits, after the commit and before the events are
//! published, so a `canonical` call that starts after `merge` returns sees
//! the merge (`reconstruct.directory.merge-visible-after-return`). Another
//! node's merges reach it as `AgentMerged` and `AgentUnmerged`, which
//! [`DirectoryCache::apply`] folds in before any later read
//! (`reconstruct.agent-directory.cache-applies-merges`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{PoisonError, RwLock};

use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::ids::AgentId;

/// Merged agent to canonical agent, and the reverse.
#[derive(Debug, Default)]
struct Entries {
    into: BTreeMap<AgentId, AgentId>,
    aliases: BTreeMap<AgentId, BTreeSet<AgentId>>,
}

impl Entries {
    fn point(&mut self, agent: AgentId, into: Option<AgentId>) {
        if let Some(previous) = self.into.remove(&agent)
            && let Some(set) = self.aliases.get_mut(&previous)
        {
            set.remove(&agent);
            if set.is_empty() {
                self.aliases.remove(&previous);
            }
        }
        if let Some(into) = into {
            self.into.insert(agent, into);
            self.aliases.entry(into).or_default().insert(agent);
        }
    }
}

/// The merge table, shared by every clone of the store.
#[derive(Debug, Default)]
pub(crate) struct DirectoryCache {
    entries: RwLock<Entries>,
}

impl DirectoryCache {
    /// The canonical agent of `id`: its target when merged, else itself.
    pub(crate) fn canonical(&self, id: AgentId) -> AgentId {
        // A poisoned lock means a writer panicked between two field
        // updates of `point`, which leave a valid map either way.
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
        entries.into.get(&id).copied().unwrap_or(id)
    }

    /// `canonical(id)` and every agent merged into it, ascending.
    pub(crate) fn members(&self, id: AgentId) -> Vec<AgentId> {
        let entries = self.entries.read().unwrap_or_else(PoisonError::into_inner);
        let canonical = entries.into.get(&id).copied().unwrap_or(id);
        let mut members: Vec<AgentId> = entries
            .aliases
            .get(&canonical)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        members.push(canonical);
        members.sort_unstable();
        members
    }

    /// Replace the whole table: `(merged agent, its target)` pairs.
    pub(crate) fn load(&self, merged: impl IntoIterator<Item = (AgentId, AgentId)>) {
        let mut fresh = Entries::default();
        for (agent, into) in merged {
            fresh.point(agent, Some(into));
        }
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        *entries = fresh;
    }

    /// Point `agent` at `into` (`None`: it is canonical again).
    pub(crate) fn point(&self, agent: AgentId, into: Option<AgentId>) {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        entries.point(agent, into);
    }

    /// Fold in a merge or unmerge another node published. Other events are
    /// ignored.
    pub(crate) fn apply(&self, event: &IngestEvent) {
        let mut entries = self.entries.write().unwrap_or_else(PoisonError::into_inner);
        match event {
            IngestEvent::AgentMerged {
                from,
                into,
                repointed,
                ..
            } => {
                for agent in std::iter::once(from).chain(repointed) {
                    entries.point(*agent, Some(*into));
                }
            }
            IngestEvent::AgentUnmerged {
                agent, restored, ..
            } => {
                entries.point(*agent, None);
                for alias in restored {
                    entries.point(*alias, Some(*agent));
                }
            }
            IngestEvent::ExchangeCaptured(_)
            | IngestEvent::ConversationDelta(_)
            | IngestEvent::AgentSeen { .. }
            | IngestEvent::AgentRenamed { .. } => {}
        }
    }
}
