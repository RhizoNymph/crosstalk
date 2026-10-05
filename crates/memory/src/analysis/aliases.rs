//! [`StaticDirectory`]: merges and supersessions a test sets directly, for
//! the stores that resolve ids at read time.
//!
//! The L3 and L5 reference stores own the real merge and supersession
//! tables; the L6–L8 stores only read them through the spec's
//! [`AgentDirectory`] and [`ChannelDirectory`], so any implementation of
//! those traits can stand behind them. This one keeps the spec's one-step
//! rule: a merge target is never itself merged, and a superseding channel is
//! never itself superseded.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;

use crate::support::lock;

/// A merge or supersession that would break the one-step rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AliasError {
    #[error("{0:?} cannot be merged into itself")]
    AgentIntoItself(AgentId),
    #[error("{0:?} is merged, so nothing can be merged into it")]
    MergedTarget(AgentId),
    #[error("{0:?} is already merged")]
    MergedSource(AgentId),
    #[error("{0:?} cannot supersede itself")]
    ChannelBySelf(ChannelId),
    #[error("{0:?} is superseded, so it cannot supersede")]
    SupersededTarget(ChannelId),
}

/// Merges and supersessions, shared by every clone.
#[derive(Debug, Clone, Default)]
pub struct StaticDirectory {
    state: Arc<Mutex<Tables>>,
}

#[derive(Debug, Default)]
struct Tables {
    merged: BTreeMap<AgentId, AgentId>,
    superseded: BTreeMap<ChannelId, ChannelId>,
}

impl StaticDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// `from` (and every agent merged into it) now resolves to `into`.
    pub fn merge(&self, from: AgentId, into: AgentId) -> Result<(), AliasError> {
        let mut tables = lock(&self.state);
        if from == into {
            return Err(AliasError::AgentIntoItself(from));
        }
        if tables.merged.contains_key(&into) {
            return Err(AliasError::MergedTarget(into));
        }
        if tables.merged.contains_key(&from) {
            return Err(AliasError::MergedSource(from));
        }
        for target in tables.merged.values_mut() {
            if *target == from {
                *target = into;
            }
        }
        tables.merged.insert(from, into);
        Ok(())
    }

    /// `agent` resolves to itself again. Agents repointed through it stay
    /// where they are, as an unmerge of one merge record leaves them.
    pub fn unmerge(&self, agent: AgentId) {
        lock(&self.state).merged.remove(&agent);
    }

    /// `channel` now resolves to `by`.
    pub fn supersede(&self, channel: ChannelId, by: ChannelId) -> Result<(), AliasError> {
        let mut tables = lock(&self.state);
        if channel == by {
            return Err(AliasError::ChannelBySelf(channel));
        }
        if tables.superseded.contains_key(&by) {
            return Err(AliasError::SupersededTarget(by));
        }
        tables.superseded.insert(channel, by);
        Ok(())
    }
}

impl AgentDirectory for StaticDirectory {
    fn canonical(&self, id: AgentId) -> AgentId {
        lock(&self.state).merged.get(&id).copied().unwrap_or(id)
    }
}

impl ChannelDirectory for StaticDirectory {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        lock(&self.state).superseded.get(&id).copied().unwrap_or(id)
    }
}

/// Both directories as the spec's [`Aliases`], for
/// `TopologyFilter::admits`, `Route::resolved` and `AlertSubject::resolved`.
#[derive(Debug)]
pub struct Directories<'a, D>(pub &'a D);

// Manual impls: a derive would require `D` itself to be `Copy`.
impl<D> Clone for Directories<'_, D> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<D> Copy for Directories<'_, D> {}

impl<D: AgentDirectory + ChannelDirectory> Aliases for Directories<'_, D> {
    fn agent(&self, id: AgentId) -> AgentId {
        AgentDirectory::canonical(self.0, id)
    }

    fn channel(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(self.0, id)
    }
}
