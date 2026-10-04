//! Agents, labels, merge history and vetoes (items 14 and 15).

use crosstalk_spec::ids::{AgentId, OperatorId};
use crosstalk_spec::observed::agent::{IdentityEvidence, MergeAuthor};
use crosstalk_spec::observed::client::{HarnessClaim, HarnessFamily};
use crosstalk_spec::support::{NonBlank, NonEmpty, Timestamp};

use crosstalk_spec::ids::MergeId;

/// An operator-chosen display name: trimmed, non-empty, at most
/// [`AgentLabel::MAX_CHARS`] characters.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentLabel(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidLabel {
    #[error("label is empty")]
    Empty,
    #[error("label is longer than {max} characters", max = AgentLabel::MAX_CHARS)]
    TooLong,
}

impl AgentLabel {
    pub const MAX_CHARS: usize = 64;

    pub fn new(raw: &str) -> Result<Self, InvalidLabel> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(InvalidLabel::Empty);
        }
        if trimmed.chars().count() > Self::MAX_CHARS {
            return Err(InvalidLabel::TooLong);
        }
        Ok(Self(trimmed.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The state of an agent that is not merged: what an unmerge returns it to.
/// `Merged` keeps one (`prior`), so a merged agent cannot have a merged
/// prior state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveAgentState {
    Registered { at: Timestamp },
    Provisional { first_seen: Timestamp },
    Established { since: Timestamp },
}

/// Replaces `crosstalk_spec::observed::agent::AgentState`: `Merged` keeps
/// the state the agent had before the merge, which an unmerge restores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentState {
    Registered {
        at: Timestamp,
    },
    Provisional {
        first_seen: Timestamp,
    },
    /// Holds evidence of at least two variants, at least one of them strong.
    Established {
        since: Timestamp,
    },
    /// This agent turned out to be `into`. Its records keep its own id and
    /// resolve to `into` at read time. `into` is never this agent and is
    /// never itself merged.
    Merged {
        into: AgentId,
        at: Timestamp,
        by: MergeAuthor,
        prior: ActiveAgentState,
    },
}

impl AgentState {
    /// The active state, or `None` when merged.
    pub fn active(&self) -> Option<ActiveAgentState> {
        match *self {
            Self::Registered { at } => Some(ActiveAgentState::Registered { at }),
            Self::Provisional { first_seen } => Some(ActiveAgentState::Provisional { first_seen }),
            Self::Established { since } => Some(ActiveAgentState::Established { since }),
            Self::Merged { .. } => None,
        }
    }

    pub fn is_merged(&self) -> bool {
        matches!(self, Self::Merged { .. })
    }
}

impl From<ActiveAgentState> for AgentState {
    fn from(state: ActiveAgentState) -> Self {
        match state {
            ActiveAgentState::Registered { at } => Self::Registered { at },
            ActiveAgentState::Provisional { first_seen } => Self::Provisional { first_seen },
            ActiveAgentState::Established { since } => Self::Established { since },
        }
    }
}

/// Replaces `crosstalk_spec::observed::agent::Agent`, holding the
/// contract's [`AgentState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    pub id: AgentId,
    pub evidence: NonEmpty<IdentityEvidence>,
    /// The agent that spawned this one, from harness parent ids in the same
    /// scope.
    pub parent: Option<AgentId>,
    pub state: AgentState,
}

/// The agent state kinds a canonical agent can be in. Graph nodes and lists
/// show canonical agents only, so `Merged` is not a kind here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgentStateKind {
    Registered,
    Provisional,
    Established,
}

/// One applied merge. `repointed` are the agents that had been merged into
/// `from` and were moved to `into` by this merge; an unmerge restores them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeRecord {
    pub id: MergeId,
    pub from: AgentId,
    pub into: AgentId,
    pub by: MergeAuthor,
    pub at: Timestamp,
    pub repointed: Vec<AgentId>,
    /// Set once an operator reverted this merge.
    pub reverted: Option<(OperatorId, Timestamp)>,
}

/// Stops the identity resolver from merging two agents an operator split.
/// An operator merge of the same pair clears it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeVeto {
    pub a: AgentId,
    pub b: AgentId,
    pub by: OperatorId,
    pub at: Timestamp,
}

/// A harness claim and when it was last seen on the agent's exchanges.
/// Always shown as a claim, never as identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimSeen {
    pub claim: HarnessClaim,
    pub last_seen: Timestamp,
}

/// What an agent is called (item 24): its canonical agent and that agent's
/// label. Asking for an alias names the agent it was merged into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentName {
    /// The canonical agent.
    pub id: AgentId,
    pub label: Option<AgentLabel>,
}

/// Restricts the agents list (item 28). Empty lists and `None` do not
/// restrict; fields combine with AND.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentListFilter {
    pub states: Vec<AgentStateKind>,
    /// Agents with a harness claim of one of these families. Claims are
    /// what clients said, not identity.
    pub harness_claims: Vec<HarnessFamily>,
    /// Matches the label or the id's text, ignoring case.
    pub text: Option<NonBlank>,
    /// Agents whose canonical parent is one of these: one level of a
    /// sub-agent tree.
    pub parents: Vec<AgentId>,
}

/// A row in the agents list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSummary {
    pub id: AgentId,
    pub label: Option<AgentLabel>,
    pub state: AgentStateKind,
    pub parent: Option<AgentId>,
    pub claims: Vec<ClaimSeen>,
    pub transmissions_in: u64,
    pub transmissions_out: u64,
    pub last_seen: Timestamp,
}

/// The agent page. `agent` is the canonical agent; `aliases` are the agents
/// merged into it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentDetail {
    pub summary: AgentSummary,
    pub agent: Agent,
    pub aliases: Vec<Agent>,
    pub children: Vec<AgentId>,
    pub merges: Vec<MergeRecord>,
    pub vetoes: Vec<MergeVeto>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_states_round_trip_and_merged_has_none() {
        let at = Timestamp::from_micros(5);
        for active in [
            ActiveAgentState::Registered { at },
            ActiveAgentState::Provisional { first_seen: at },
            ActiveAgentState::Established { since: at },
        ] {
            let state = AgentState::from(active);
            assert_eq!(state.active(), Some(active));
            assert!(!state.is_merged());
        }
        let merged = AgentState::Merged {
            into: AgentId::from_ulid(2),
            at,
            by: MergeAuthor::Resolver,
            prior: ActiveAgentState::Provisional { first_seen: at },
        };
        assert_eq!(merged.active(), None);
        assert!(merged.is_merged());
    }
}
