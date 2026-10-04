//! Merge, unmerge and rename, applied to the merge table as
//! `IdentityResolver` defines them, refusals mapped as the surface maps
//! `ResolveError` (`ActionError::from`).

use crosstalk_spec::ids::{AgentId, MergeId, OperatorId};
use crosstalk_spec::interfaces::l8_surface::{ActionError, QueryError};
use crosstalk_spec::observed::agent::{AgentLabel, MergeAuthor, MergeRequest};

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::identity::refusal;
use crate::backend::fixture::store::State;
use crate::contract::actions::ActionOutcome;

/// Merges the request's source into its target under a new record,
/// authored by the caller (the surface stamps the author). Both must be
/// canonical (`Conflict(AgentMerged)`); two ids of one cluster are
/// `Conflict(MergeIntoSelf)`, checked first. An operator merge clears the
/// vetoes between the two clusters.
pub fn merge(state: &mut State, by: OperatorId, request: &MergeRequest) -> Result<ActionOutcome> {
    let stamped = MergeRequest::new(
        request.source(),
        request.target(),
        MergeAuthor::Operator(by),
    )
    .map_err(|e| QueryError::from(ActionError::from(e)))?;
    let id = MergeId::from_ulid(state.mint.ulid(NOW));
    let record = state.identity.merge(id, stamped, NOW).map_err(refusal)?;
    Ok(ActionOutcome::Merged(record.id()))
}

/// Reverts exactly one merge record; a second revert is
/// `Conflict(MergeAlreadyReverted)`.
pub fn unmerge(state: &mut State, by: OperatorId, merge: MergeId) -> Result<ActionOutcome> {
    state.identity.unmerge(merge, by, NOW).map_err(refusal)?;
    Ok(ActionOutcome::Applied)
}

/// Sets or clears an active agent's label; a merged agent is
/// `Conflict(AgentMerged)`.
pub fn rename(
    state: &mut State,
    agent: AgentId,
    label: &Option<AgentLabel>,
) -> Result<ActionOutcome> {
    state
        .identity
        .rename(agent, label.clone())
        .map_err(refusal)?;
    Ok(ActionOutcome::Applied)
}
