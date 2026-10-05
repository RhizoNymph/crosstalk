//! The `Changed` notifications the fixture's stores publish after an
//! action commits, as `events::changed` lists them per store.
//!
//! Most come from the action and its outcome: the channel a decision was
//! recorded for, a promotion's channel and every channel it superseded
//! (`Changed::promotion`), a merge's source, target and repointed agents,
//! an unmerge's source, its former target and the agents it restored, a
//! renamed agent, a judged transmission, a created, updated, enabled or
//! disabled rule. Two stores change as a side effect, so they are read by
//! comparing the state before and after: every alert whose state changed
//! (acknowledged, resolved, or suppressed by a sanction, a disabled rule or
//! a false detection) and every topic version whose catalog entry changed
//! (pinned, unpinned, dropped by the retention an unpin runs). An action
//! that is refused or changes nothing publishes nothing.

use crosstalk_spec::aggregates::alert::AlertState;
use crosstalk_spec::aggregates::topic_history::TopicVersionHistory;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::interfaces::l8_surface::{ActionOutcome, OperatorAction};

use super::Acted;
use crate::backend::fixture::store::State;

/// What the side-effect stores held before the action.
#[derive(Debug, Clone)]
pub struct Before {
    alerts: Vec<AlertState>,
    catalog: TopicVersionHistory,
}

impl Before {
    pub fn of(state: &State) -> Self {
        Self {
            alerts: state
                .alerts
                .iter()
                .map(|alert| alert.state.clone())
                .collect(),
            catalog: state.catalog.clone(),
        }
    }
}

/// What the stores publish after `action` returned `result`.
pub fn changes(
    before: &Before,
    state: &State,
    action: &OperatorAction,
    result: &Acted,
) -> Vec<Changed> {
    let Ok(outcome) = result else {
        return Vec::new();
    };
    if *outcome == ActionOutcome::Unchanged {
        return Vec::new();
    }
    let mut out = named(state, action, outcome);
    for (index, alert) in state.alerts.iter().enumerate() {
        if before.alerts.get(index) != Some(&alert.state) {
            out.push(Changed::Alert(alert.id));
        }
    }
    for info in state.catalog.versions() {
        if before.catalog.get(info.version()) != Some(info) {
            out.push(Changed::TopicVersion(info.version()));
        }
    }
    let mut seen = std::collections::HashSet::new();
    out.retain(|changed| seen.insert(*changed));
    out
}

/// The entities the action itself names as changed.
fn named(state: &State, action: &OperatorAction, outcome: &ActionOutcome) -> Vec<Changed> {
    match action {
        OperatorAction::SetPolicy { channel, .. } => vec![Changed::Channel(*channel)],
        OperatorAction::PromoteChannel { channel, .. } => match outcome {
            ActionOutcome::ChannelPromoted {
                channel,
                superseded,
            } => Changed::promotion(*channel, superseded.as_slice()),
            ActionOutcome::Applied
            | ActionOutcome::Unchanged
            | ActionOutcome::RuleCreated(_)
            | ActionOutcome::Merged(_) => vec![Changed::Channel(*channel)],
        },
        OperatorAction::MergeAgents(request) => {
            let record = match outcome {
                ActionOutcome::Merged(id) => state.identity.merges().iter().find(|r| r.id() == *id),
                ActionOutcome::Applied
                | ActionOutcome::Unchanged
                | ActionOutcome::RuleCreated(_)
                | ActionOutcome::ChannelPromoted { .. } => None,
            };
            let mut agents = vec![request.source(), request.target()];
            agents.extend(record.map_or(&[][..], |record| record.repointed()));
            agents.into_iter().map(Changed::Agent).collect()
        }
        OperatorAction::Unmerge { merge } => state
            .identity
            .merges()
            .iter()
            .find(|record| record.id() == *merge)
            .map(|record| {
                let restored = record
                    .reverted()
                    .map_or(&[][..], |reversal| reversal.restored.as_slice());
                [record.source(), record.target()]
                    .into_iter()
                    .chain(restored.iter().copied())
                    .map(Changed::Agent)
                    .collect()
            })
            .unwrap_or_default(),
        OperatorAction::RenameAgent { agent, .. } => vec![Changed::Agent(*agent)],
        OperatorAction::SetVerdict { transmission, .. } => vec![Changed::Verdict(*transmission)],
        OperatorAction::CreateRule { .. } => match outcome {
            ActionOutcome::RuleCreated(rule) => vec![Changed::Rule(*rule)],
            ActionOutcome::Applied
            | ActionOutcome::Unchanged
            | ActionOutcome::ChannelPromoted { .. }
            | ActionOutcome::Merged(_) => Vec::new(),
        },
        OperatorAction::UpdateRule { id, .. } | OperatorAction::SetRuleEnabled { id, .. } => {
            vec![Changed::Rule(*id)]
        }
        // Alerts and topic versions are read from the state; dead letters
        // are not a feed entity.
        OperatorAction::Acknowledge { .. }
        | OperatorAction::Resolve { .. }
        | OperatorAction::PinTopicVersion { .. }
        | OperatorAction::UnpinTopicVersion { .. }
        | OperatorAction::ReplayDeadLetter { .. } => Vec::new(),
    }
}
