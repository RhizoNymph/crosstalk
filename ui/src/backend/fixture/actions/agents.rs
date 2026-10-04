//! Merge, unmerge and rename (items 14 and 15).

use crosstalk_spec::ids::{AgentId, OperatorId};
use crosstalk_spec::observed::agent::{AgentState, MergeAuthor, MergeRequest};

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::store::{State, active_state, from_active};
use crate::contract::MergeId;
use crate::contract::actions::ActionOutcome;
use crate::contract::agents::{AgentLabel, MergeRecord, MergeVeto};
use crate::contract::errors::{ConflictKind, InputError, QueryError};

fn exists(state: &State, id: AgentId) -> Result<()> {
    if state.agents.contains_key(&id) {
        Ok(())
    } else {
        Err(QueryError::NotFound)
    }
}

/// Merges the source into the target's canonical agent, repoints agents
/// already merged into the source, and clears any veto on the pair.
pub fn merge(state: &mut State, by: OperatorId, request: &MergeRequest) -> Result<ActionOutcome> {
    let (source, target) = (request.source(), request.target());
    exists(state, source)?;
    exists(state, target)?;
    let prior = state
        .agents
        .get(&source)
        .and_then(|r| active_state(&r.agent.state))
        .ok_or(QueryError::Conflict(ConflictKind::AgentMerged))?;
    let into = state.canonical_agent(target);
    if into == source {
        return Err(QueryError::InvalidInput(InputError::Field {
            field: "into",
            reason: "the target is merged into the source".to_owned(),
        }));
    }
    let repointed: Vec<AgentId> = state
        .agents
        .values()
        .filter(|r| matches!(r.agent.state, AgentState::Merged { into: i, .. } if i == source))
        .map(|r| r.agent.id)
        .collect();
    for id in &repointed {
        if let Some(AgentState::Merged { into: slot, .. }) =
            state.agents.get_mut(id).map(|r| &mut r.agent.state)
        {
            *slot = into;
        }
    }
    let author = MergeAuthor::Operator(by);
    if let Some(record) = state.agents.get_mut(&source) {
        record.agent.state = AgentState::Merged {
            into,
            at: NOW,
            by: author,
        };
    }
    let pair =
        |v: &MergeVeto, x: AgentId| (v.a == source && v.b == x) || (v.a == x && v.b == source);
    state.vetoes.retain(|v| !pair(v, target) && !pair(v, into));
    let id = MergeId::from_ulid(state.mint.ulid(NOW));
    state.merge_priors.insert(id, prior);
    state.merges.push(MergeRecord {
        id,
        from: source,
        into,
        by: author,
        at: NOW,
        repointed,
        reverted: None,
    });
    Ok(ActionOutcome::Merged(id))
}

/// Reverts exactly one merge: the merged agent gets its prior state back,
/// the agents that merge repointed point at it again, and a veto stops the
/// resolver from merging the pair again.
pub fn unmerge(state: &mut State, by: OperatorId, merge: MergeId) -> Result<ActionOutcome> {
    let index = state
        .merges
        .iter()
        .position(|m| m.id == merge)
        .ok_or(QueryError::NotFound)?;
    let record = state.merges[index].clone();
    if record.reverted.is_some() {
        return Err(QueryError::Conflict(ConflictKind::MergeReverted));
    }
    let prior = state
        .merge_priors
        .get(&merge)
        .copied()
        .ok_or_else(|| QueryError::Store {
            reason: "merge has no recorded prior state".to_owned(),
        })?;
    if let Some(agent) = state.agents.get_mut(&record.from) {
        agent.agent.state = from_active(prior);
    }
    for id in &record.repointed {
        if let Some(AgentState::Merged { into, .. }) =
            state.agents.get_mut(id).map(|r| &mut r.agent.state)
        {
            *into = record.from;
        }
    }
    state.merges[index].reverted = Some((by, NOW));
    state.vetoes.push(MergeVeto {
        a: record.from,
        b: record.into,
        by,
        at: NOW,
    });
    Ok(ActionOutcome::Applied)
}

pub fn rename(
    state: &mut State,
    agent: AgentId,
    label: &Option<AgentLabel>,
) -> Result<ActionOutcome> {
    exists(state, agent)?;
    if state.is_merged(agent) {
        return Err(QueryError::Conflict(ConflictKind::AgentMerged));
    }
    if let Some(record) = state.agents.get_mut(&agent) {
        record.label = label.clone();
    }
    Ok(ActionOutcome::Applied)
}
