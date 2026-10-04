//! Operator actions. Every call, applied or rejected, appends one audit
//! entry; authors and times are stamped here from the caller and the
//! fixture's clock, never taken from the request.

mod agents;
mod channels;
pub mod effects;
mod rules;
mod triage;

pub use triage::record_verdict;

use crosstalk_spec::interfaces::l8_surface::Caller;

use crate::backend::Result;
use crate::contract::actions::{ActionOutcome, OperatorAction};
use crate::contract::research::{Actor, AuditOutcome, AuditedAction};

use super::clock::NOW;
use super::queries::require;
use super::store::State;
use super::world::World;

fn permitted(caller: &Caller, action: &OperatorAction) -> Result<()> {
    require(caller, action.requires())?;
    if let Some(extra) = action.also_requires() {
        require(caller, extra)?;
    }
    Ok(())
}

fn apply(
    world: &World,
    state: &mut State,
    caller: &Caller,
    action: &OperatorAction,
) -> Result<ActionOutcome> {
    let by = caller.operator();
    match action {
        OperatorAction::SetPolicy {
            channel,
            policy,
            note,
        } => channels::set_policy(state, by, *channel, *policy, note.clone()),
        OperatorAction::PromoteChannel {
            channel,
            pattern,
            policy,
            note,
        } => channels::promote(world, state, by, *channel, pattern, *policy, note.clone()),
        OperatorAction::MergeAgents(request) => agents::merge(state, by, request),
        OperatorAction::Unmerge { merge } => agents::unmerge(state, by, *merge),
        OperatorAction::RenameAgent { agent, label } => agents::rename(state, *agent, label),
        OperatorAction::SetVerdict {
            transmission,
            verdict,
            note,
        } => triage::set_verdict(world, state, by, *transmission, *verdict, note.clone()),
        OperatorAction::Acknowledge { alert } => triage::acknowledge(state, by, *alert),
        OperatorAction::Resolve { alert, note } => triage::resolve(state, by, *alert, note.clone()),
        OperatorAction::ReplayDeadLetter { group, id } => triage::replay(state, group, *id),
        OperatorAction::CreateRule { name, rule, sinks } => {
            rules::create(world, state, by, name, rule, sinks)
        }
        OperatorAction::UpdateRule {
            id,
            name,
            rule,
            sinks,
        } => rules::update(world, state, *id, name, rule, sinks),
        OperatorAction::SetRuleEnabled { id, status } => rules::set_enabled(state, *id, *status),
    }
}

/// Checks permissions, applies the action and audits the outcome.
pub fn act(
    world: &World,
    state: &mut State,
    caller: &Caller,
    action: OperatorAction,
) -> Result<ActionOutcome> {
    let result = permitted(caller, &action).and_then(|()| apply(world, state, caller, &action));
    let subject = effects::subject(&action, result.as_ref().ok());
    let outcome = match &result {
        Ok(outcome) => AuditOutcome::Applied(*outcome),
        Err(err) => AuditOutcome::Rejected(err.clone()),
    };
    effects::audit(
        state,
        NOW,
        Actor::Operator(caller.operator()),
        AuditedAction::Operator(action),
        subject,
        outcome,
    );
    result
}
