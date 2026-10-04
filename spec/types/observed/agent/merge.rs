//! The merge log, exact unmerges and merge vetoes.
//!
//! Every merge appends one [`MergeRecord`]. A record never changes after it
//! is written, except that an unmerge marks it reverted ([`MergeRecord::revert`]).
//!
//! **Merging `from` into `into`** (`IdentityResolver::merge`):
//! 1. [`MergeRequest::conflict`] holds no conflict. The two agents resolve
//!    to different canonical agents: two different ids of one cluster (one
//!    merged into the other, or both into a third) are refused as
//!    [`MergeConflict::IntoSelf`], since merging a cluster into itself has
//!    no meaning whichever ids name it. Then both agents are canonical (not
//!    `Merged`), so the record names the target every reader resolves to
//!    and no chain is ever longer than one: a request naming a merged agent
//!    is refused as [`MergeConflict::Merged`]. A request naming one id
//!    twice cannot be built at all ([`MergeRequest::new`] returns
//!    `SelfMerge`); `IntoSelf` is the same refusal once aliases are
//!    resolved, which needs the merge table.
//! 2. `from` becomes [`AgentState::Merged`] with its active state as
//!    `prior` ([`Agent::merge_away`]).
//! 3. Every agent merged into `from` is repointed to `into`
//!    ([`Agent::repoint`]) and listed in the record's `repointed`.
//!
//! **Reverting record `m`** (`IdentityResolver::unmerge`):
//! 1. `m` is not reverted yet; reverting it again is refused.
//! 2. `m.from` returns to its `prior` state ([`Agent::revert`]). While `m` is
//!    unreverted, `m.from` is merged by `m` and by no other record: the only
//!    way out of `Merged` is reverting the record that put it there.
//! 3. Every agent in `m.repointed` that `m` repointed and nothing has moved
//!    since points at `m.from` again ([`Agent::restore`]); later repoints of
//!    it are forgotten, because they followed the target `m` gave it. An
//!    agent unmerged or merged afresh since `m` is left alone.
//! 4. A [`MergeVeto`] between `m.from` and `m.into` is recorded.
//!
//! Records can be reverted in any order. Reverting the latest record undoes
//! it exactly, so a merge followed by its revert leaves every agent's state
//! as it was.
//!
//! **Vetoes.** The resolver merges agents when it finds the same strong
//! evidence on both. After an operator splits them, that evidence is still
//! there, so without a veto the next exchange would merge them again. The
//! resolver refuses a merge between two clusters (a canonical agent and the
//! agents merged into it) that a veto separates ([`MergeVeto::separates`]).
//! An operator merge between them is a deliberate decision: it goes ahead
//! and deletes those vetoes.

use serde::{Deserialize, Serialize};

use crate::ids::{AgentId, MergeId, OperatorId};
use crate::support::Timestamp;
use crate::wire::Rejected;

use super::{ActiveAgentState, Agent, AgentState, MergeAuthor, MergeRequest, SelfMerge};

/// Why the merge table refuses a [`MergeRequest`], whoever asked for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeConflict {
    /// The request's two agents already resolve to `canonical`: one is
    /// merged into the other, or both into `canonical`.
    IntoSelf { canonical: AgentId },
    /// `agent`, named by the request, is merged into `into`; the request
    /// should name `into`. Only returned when the two agents resolve to
    /// different canonical agents.
    Merged { agent: AgentId, into: AgentId },
}

impl MergeRequest {
    /// Whether the merge table refuses this request, given the current
    /// states of its source and target: [`MergeConflict::IntoSelf`] when
    /// both resolve to one canonical agent, else
    /// [`MergeConflict::Merged`] for a merged source, then a merged target;
    /// `None` when both are canonical. Vetoes are checked after this, and
    /// only for resolver merges.
    pub fn conflict(&self, source: &AgentState, target: &AgentState) -> Option<MergeConflict> {
        let from = source.merged_into().unwrap_or(self.from);
        let into = target.merged_into().unwrap_or(self.into);
        if from == into {
            return Some(MergeConflict::IntoSelf { canonical: from });
        }
        if let Some(canonical) = source.merged_into() {
            return Some(MergeConflict::Merged {
                agent: self.from,
                into: canonical,
            });
        }
        target.merged_into().map(|canonical| MergeConflict::Merged {
            agent: self.into,
            into: canonical,
        })
    }
}

/// One merge: `from` (read with [`MergeRecord::source`]) merged into `into`
/// ([`MergeRecord::target`]). Built only through [`MergeRecord::new`];
/// `from` and `into` differ because a [`MergeRequest`] cannot name the same
/// agent twice. The accessors are not named `from` and `into` because those
/// would shadow the conversion traits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawMergeRecord")]
pub struct MergeRecord {
    id: MergeId,
    from: AgentId,
    into: AgentId,
    by: MergeAuthor,
    at: Timestamp,
    repointed: Vec<AgentId>,
    reverted: Option<Reversal>,
}

/// [`MergeRecord`]'s fields, decoded without the checks. Decoding goes
/// through the record's constructors in the order the log applies them:
/// [`MergeRequest::new`] (a self-merge is refused), [`MergeRecord::new`],
/// then [`MergeRecord::revert`] with the reversal, if any. A record carries
/// one `reverted` slot, so the second reversal `revert` refuses cannot be
/// written (a repeated key is a decode error).
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawMergeRecord {
    id: MergeId,
    from: AgentId,
    into: AgentId,
    by: MergeAuthor,
    at: Timestamp,
    repointed: Vec<AgentId>,
    reverted: Option<Reversal>,
}

impl TryFrom<RawMergeRecord> for MergeRecord {
    type Error = Rejected<InvalidMergeRecord>;

    fn try_from(raw: RawMergeRecord) -> Result<Self, Self::Error> {
        let request = MergeRequest::new(raw.from, raw.into, raw.by)
            .map_err(|SelfMerge| Rejected::new("merge record", InvalidMergeRecord::SelfMerge))?;
        let mut record = Self::new(raw.id, request, raw.at, raw.repointed);
        if let Some(reversal) = raw.reverted {
            record.revert(reversal).map_err(|error| {
                Rejected::new("merge record", InvalidMergeRecord::AlreadyReverted(error))
            })?;
        }
        Ok(record)
    }
}

/// Why a stored [`MergeRecord`] does not decode: what its constructors
/// refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidMergeRecord {
    /// [`MergeRequest::new`]: the record names one agent as source and
    /// target.
    SelfMerge,
    /// [`MergeRecord::revert`]: a second reversal of the record.
    AlreadyReverted(AlreadyReverted),
}

/// An operator's unmerge of one record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct Reversal {
    pub by: OperatorId,
    pub at: Timestamp,
    /// The agents of the record's `repointed` that now point at its `from`
    /// again, in the record's order.
    pub restored: Vec<AgentId>,
}

/// A revert of a record that was already reverted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlreadyReverted {
    pub merge: MergeId,
}

impl MergeRecord {
    /// The record of `request`, with the agents it repointed from the
    /// source to the target.
    pub fn new(id: MergeId, request: MergeRequest, at: Timestamp, repointed: Vec<AgentId>) -> Self {
        Self {
            id,
            from: request.source(),
            into: request.target(),
            by: request.by(),
            at,
            repointed,
            reverted: None,
        }
    }

    pub fn id(&self) -> MergeId {
        self.id
    }

    /// The agent merged away.
    pub fn source(&self) -> AgentId {
        self.from
    }

    /// The canonical agent it was merged into.
    pub fn target(&self) -> AgentId {
        self.into
    }

    pub fn by(&self) -> MergeAuthor {
        self.by
    }

    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// The agents merged into `from` that this merge repointed to `into`.
    pub fn repointed(&self) -> &[AgentId] {
        &self.repointed
    }

    pub fn reverted(&self) -> Option<&Reversal> {
        self.reverted.as_ref()
    }

    /// Mark the record reverted. Refuses a second revert and keeps the
    /// first.
    pub fn revert(&mut self, reversal: Reversal) -> Result<(), AlreadyReverted> {
        if self.reverted.is_some() {
            return Err(AlreadyReverted { merge: self.id });
        }
        self.reverted = Some(reversal);
        Ok(())
    }
}

/// A merged agent's state: the record that merged it, where it resolves to
/// now, and what an unmerge restores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct MergedInto {
    /// The record that merged this agent. Reverting it unmerges the agent.
    pub merge: MergeId,
    /// The canonical agent this one resolves to. Never this agent, never
    /// merged.
    pub into: AgentId,
    /// The state this agent was in when it was merged.
    pub prior: ActiveAgentState,
    /// The later merges that repointed this agent, oldest first. Empty for
    /// an agent still pointing at the target its own merge gave it.
    pub repointed_by: Vec<MergeId>,
}

/// A transition refused because the agent is not in the state it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidMergeTransition {
    /// The record's source is another agent.
    OtherAgent,
    /// The agent is already merged (by any record), so it cannot be merged
    /// again.
    AlreadyMerged { into: AgentId },
    /// The agent is not merged by this record.
    NotMergedByRecord,
}

impl Agent {
    /// Merge this agent away under `record`: `Merged` with its active state
    /// as `prior`. Refuses an agent that is not the record's source or is
    /// already merged, changing nothing.
    pub fn merge_away(&mut self, record: &MergeRecord) -> Result<(), InvalidMergeTransition> {
        if self.id != record.from {
            return Err(InvalidMergeTransition::OtherAgent);
        }
        let prior = self
            .state
            .active()
            .map_err(|merged| InvalidMergeTransition::AlreadyMerged { into: merged.into })?;
        self.state = AgentState::Merged(MergedInto {
            merge: record.id,
            into: record.into,
            prior,
            repointed_by: Vec::new(),
        });
        Ok(())
    }

    /// `record` merged this agent's target away: point at the record's
    /// target. Returns whether it changed: only an agent merged into the
    /// record's source moves.
    pub fn repoint(&mut self, record: &MergeRecord) -> bool {
        match &mut self.state {
            AgentState::Merged(merged) if merged.into == record.from => {
                merged.repointed_by.push(record.id);
                merged.into = record.into;
                true
            }
            AgentState::Merged(_)
            | AgentState::Registered { .. }
            | AgentState::Provisional { .. }
            | AgentState::Established { .. } => false,
        }
    }

    /// Return the source of `record` to the state it was merged from.
    /// Refuses, changing nothing, unless this agent is merged by `record`.
    pub fn revert(&mut self, record: &MergeRecord) -> Result<(), InvalidMergeTransition> {
        if self.id != record.from {
            return Err(InvalidMergeTransition::OtherAgent);
        }
        match &self.state {
            AgentState::Merged(merged) if merged.merge == record.id => {
                self.state = merged.prior.into();
                Ok(())
            }
            AgentState::Merged(_)
            | AgentState::Registered { .. }
            | AgentState::Provisional { .. }
            | AgentState::Established { .. } => Err(InvalidMergeTransition::NotMergedByRecord),
        }
    }

    /// `record` was reverted. If `record` repointed this agent and it has
    /// not been unmerged or merged afresh since, point it at the record's
    /// source again and forget the repoints after it. Returns whether it
    /// changed.
    pub fn restore(&mut self, record: &MergeRecord) -> bool {
        match &mut self.state {
            AgentState::Merged(merged) => {
                match merged.repointed_by.iter().position(|m| *m == record.id) {
                    Some(index) => {
                        merged.repointed_by.truncate(index);
                        merged.into = record.from;
                        true
                    }
                    None => false,
                }
            }
            AgentState::Registered { .. }
            | AgentState::Provisional { .. }
            | AgentState::Established { .. } => false,
        }
    }
}

/// An operator's statement that two agents are different, recorded when
/// they unmerged them. Built only through [`MergeVeto::new`], which rejects
/// an agent paired with itself and stores the pair in order, so `(a, b)` and
/// `(b, a)` are the same veto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", try_from = "RawMergeVeto")]
pub struct MergeVeto {
    a: AgentId,
    b: AgentId,
    by: OperatorId,
    at: Timestamp,
}

/// [`MergeVeto`]'s fields, decoded without the check. Decoding goes through
/// [`MergeVeto::new`], which orders the pair.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawMergeVeto {
    a: AgentId,
    b: AgentId,
    by: OperatorId,
    at: Timestamp,
}

impl TryFrom<RawMergeVeto> for MergeVeto {
    type Error = Rejected<SelfMerge>;

    fn try_from(raw: RawMergeVeto) -> Result<Self, Self::Error> {
        Self::new(raw.a, raw.b, raw.by, raw.at).map_err(|error| Rejected::new("merge veto", error))
    }
}

impl MergeVeto {
    pub fn new(
        first: AgentId,
        second: AgentId,
        by: OperatorId,
        at: Timestamp,
    ) -> Result<Self, SelfMerge> {
        if first == second {
            return Err(SelfMerge);
        }
        let (a, b) = if first < second {
            (first, second)
        } else {
            (second, first)
        };
        Ok(Self { a, b, by, at })
    }

    /// The veto an unmerge of `record` records: between its source and its
    /// target, by the reverting operator.
    pub fn of(record: &MergeRecord, reversal: &Reversal) -> Self {
        Self {
            a: record.from.min(record.into),
            b: record.from.max(record.into),
            by: reversal.by,
            at: reversal.at,
        }
    }

    /// The lower id of the pair.
    pub fn a(&self) -> AgentId {
        self.a
    }

    /// The higher id of the pair.
    pub fn b(&self) -> AgentId {
        self.b
    }

    pub fn by(&self) -> OperatorId {
        self.by
    }

    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// Whether this veto keeps the two clusters apart: one end is in `left`
    /// and the other in `right`. A cluster is a canonical agent and every
    /// agent merged into it.
    pub fn separates(&self, left: &[AgentId], right: &[AgentId]) -> bool {
        (left.contains(&self.a) && right.contains(&self.b))
            || (left.contains(&self.b) && right.contains(&self.a))
    }
}
