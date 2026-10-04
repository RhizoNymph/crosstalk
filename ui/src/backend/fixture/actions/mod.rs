//! Operator actions as `OperatorActions::act` defines them: the caller's
//! permission is checked before any effect, the action is applied with the
//! author and time stamped here (the caller's operator and the time the
//! fixture accepted it, never taken from the request), and every call,
//! applied, unchanged, rejected or forbidden, appends one audit entry whose
//! outcome is what the call returned.

mod agents;
mod channels;
pub mod effects;
mod pins;
pub mod rules;
mod triage;

pub use triage::record_verdict;

use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, Caller, OperatorAction};
use crosstalk_spec::support::{Change, Timestamp};

use crate::contract::research::{Actor, AuditOutcome, AuditedAction};

use super::clock::NOW;
use super::store::State;
use super::world::World;

/// What `act` returns.
pub type Acted = Result<ActionOutcome, ActionError>;

/// Who an applied action is recorded as made by, and when: the caller's
/// operator and the time the action was accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub by: OperatorId,
    pub at: Timestamp,
}

/// A store's `Change` in the surface's words.
pub fn outcome_of(change: Change) -> ActionOutcome {
    match change {
        Change::Applied => ActionOutcome::Applied,
        Change::Unchanged => ActionOutcome::Unchanged,
    }
}

/// `Forbidden` naming the action's one required permission, unless the
/// caller holds it.
fn permitted(caller: &Caller, action: &OperatorAction) -> Result<(), ActionError> {
    let required = action.required_permission();
    if caller.has(required) {
        Ok(())
    } else {
        Err(ActionError::Forbidden { missing: required })
    }
}

/// Applies a permitted action. A refusal changes nothing.
fn apply(world: &World, state: &mut State, stamp: Stamp, action: &OperatorAction) -> Acted {
    match action {
        OperatorAction::SetPolicy {
            channel,
            policy,
            note,
        } => channels::set_policy(state, stamp, *channel, *policy, note.clone()),
        OperatorAction::PromoteChannel {
            channel,
            pattern,
            policy,
            note,
        } => channels::promote(
            world,
            state,
            stamp,
            *channel,
            pattern,
            *policy,
            note.clone(),
        ),
        OperatorAction::MergeAgents(request) => agents::merge(state, stamp, request),
        OperatorAction::Unmerge { merge } => agents::unmerge(state, stamp, *merge),
        OperatorAction::RenameAgent { agent, label } => agents::rename(state, *agent, label),
        OperatorAction::SetVerdict {
            transmission,
            verdict,
            note,
        } => triage::set_verdict(world, state, stamp, *transmission, *verdict, note.clone()),
        OperatorAction::Acknowledge { alert } => triage::acknowledge(state, stamp, *alert),
        OperatorAction::Resolve { alert, note } => {
            triage::resolve(state, stamp, *alert, note.clone())
        }
        OperatorAction::ReplayDeadLetter { group, id } => triage::replay(state, group, *id),
        OperatorAction::CreateRule { name, rule, sinks } => {
            rules::create(world, state, stamp, name, rule, sinks)
        }
        OperatorAction::UpdateRule {
            id,
            name,
            rule,
            sinks,
        } => rules::update(world, state, *id, name, rule, sinks),
        OperatorAction::SetRuleEnabled { id, enabled } => {
            rules::set_enabled(state, *id, *enabled, stamp.at)
        }
        OperatorAction::PinTopicVersion { version } => pins::pin(state, stamp, *version),
        OperatorAction::UnpinTopicVersion { version } => pins::unpin(state, stamp.at, *version),
    }
}

/// Checks the permission, applies the action and audits the outcome.
pub fn act(world: &World, state: &mut State, caller: &Caller, action: OperatorAction) -> Acted {
    let stamp = Stamp {
        by: caller.operator(),
        at: NOW,
    };
    let result = permitted(caller, &action).and_then(|()| apply(world, state, stamp, &action));
    let subject = effects::subject(&action, result.as_ref().ok());
    let outcome = match &result {
        Ok(outcome) => AuditOutcome::Applied(outcome.clone()),
        Err(error) => AuditOutcome::Rejected(error.clone().into()),
    };
    effects::audit(
        state,
        stamp.at,
        Actor::Operator(stamp.by),
        AuditedAction::Operator(action),
        subject,
        outcome,
    );
    result
}
