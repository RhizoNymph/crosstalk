//! Agent labels, merge history and vetoes (items 14 and 15).

use crosstalk_spec::ids::{AgentId, OperatorId};
use crosstalk_spec::observed::agent::{Agent, MergeAuthor};
use crosstalk_spec::observed::client::HarnessClaim;
use crosstalk_spec::support::Timestamp;

use super::MergeId;

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
