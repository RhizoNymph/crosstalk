//! Merge, unmerge and rename, applied to the merge table as
//! `IdentityResolver` defines them, refusals mapped as the surface maps
//! `ResolveError` (`ActionError::from`).

use crosstalk_spec::ids::{AgentId, MergeId};
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome};
use crosstalk_spec::observed::agent::{AgentLabel, MergeAuthor, MergeRequest};

use crate::backend::fixture::store::State;

use super::{Acted, Stamp, outcome_of};

/// Merges the request's source into its target under a new record,
/// authored by the caller at the acceptance time: the surface stamps the
/// author, whatever the request carried. Both must be canonical
/// (`Conflict(AgentMerged)`); two ids of one cluster are
/// `Conflict(MergeIntoSelf)`, checked first. An operator merge clears the
/// vetoes between the two clusters.
pub fn merge(state: &mut State, stamp: Stamp, request: &MergeRequest) -> Acted {
    let stamped = MergeRequest::new(
        request.source(),
        request.target(),
        MergeAuthor::Operator(stamp.by),
    )?;
    let id = MergeId::from_ulid(state.mint.ulid(stamp.at));
    let record = state
        .identity
        .merge(id, stamped, stamp.at)
        .map_err(ActionError::from)?;
    Ok(ActionOutcome::Merged(record.id()))
}

/// Reverts exactly one merge record; a second revert is
/// `Conflict(MergeAlreadyReverted)`.
pub fn unmerge(state: &mut State, stamp: Stamp, merge: MergeId) -> Acted {
    state
        .identity
        .unmerge(merge, stamp.by, stamp.at)
        .map_err(ActionError::from)?;
    Ok(ActionOutcome::Applied)
}

/// Sets or clears an active agent's label (`Unchanged` when it already
/// was); a merged agent is `Conflict(AgentMerged)`.
pub fn rename(state: &mut State, agent: AgentId, label: &Option<AgentLabel>) -> Acted {
    state
        .identity
        .rename(agent, label.clone())
        .map(outcome_of)
        .map_err(ActionError::from)
}
