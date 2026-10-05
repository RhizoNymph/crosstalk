//! The operator history: policy decisions, the promotion, merges and the
//! revert, renames, triage, verdicts (one withdrawn) and two refused
//! actions. Every past operator call is in the audit log as the spec
//! records one: an `OperatorRecord` of the caller the directory gave the
//! operator, the action and its outcome. What config made is in
//! [`super::config`], the dead letters in [`super::letters`].

use crosstalk_spec::derived::flow::channel::policy::PolicyAuthor;
use crosstalk_spec::derived::provenance::matching::MatchKind;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::actions::SupersededChannels;
use crosstalk_spec::interfaces::l8_surface::audit::{AuditOutcome, OperatorRecord};
use crosstalk_spec::interfaces::l8_surface::{
    ActionError, ActionOutcome, ConflictKind, OperatorAction, PolicyKind,
};
use crosstalk_spec::observed::agent::{MergeAuthor, MergeRequest};
use crosstalk_spec::support::Timestamp;

use crate::backend::fixture::actions::effects;
use crate::backend::fixture::clock::{DAY, HOUR, MINUTE, NOW, START, ago, minus, plus};
use crate::backend::fixture::rng::Rng;
use crate::backend::fixture::store::State;
use crosstalk_spec::aggregates::alert::AlertState;
use crosstalk_spec::derived::flow::verdict::{TransmissionVerdict, Verdict, VerdictRecorded};

use super::channels::{ChannelKey, ChannelPlan};
use super::config::caller;
use super::drafts::{decisions, team_notes_promotion};
use super::states::confirmed;
use super::{GenError, World};

/// When the deployment's configuration was first applied.
pub const CONFIG_AT: Timestamp = minus(START, 30 * DAY);

/// The researcher: every permission. The same id as the trusted operator in
/// `ui/config.json`, so the UI's own actions sit next to this history.
pub const OPERATOR_RESEARCHER: OperatorId =
    OperatorId::from_ulid(0x0192_7f71_f10d_0000_0000_0000_0000_0001);
/// The on-call triager: view, content and triage only.
pub const OPERATOR_ONCALL: OperatorId =
    OperatorId::from_ulid(0x0192_7f71_f10d_0000_0000_0000_0000_0002);

/// Records one past operator call as the surface records it: the caller
/// the directory gives `by`, the action, and `AuditOutcome::of` what the
/// call returned, at `at`.
fn operator_call(
    world: &World,
    state: &mut State,
    at: Timestamp,
    by: OperatorId,
    action: OperatorAction,
    result: &Result<ActionOutcome, ActionError>,
) -> Result<(), GenError> {
    let record = OperatorRecord::new(caller(world, by)?, action, AuditOutcome::of(result))
        .map_err(|e| GenError::invalid("OperatorRecord", e))?;
    state
        .audit
        .operator(&mut state.mint, at, record)
        .map_err(|e| GenError::invalid("operator audit entry", e))?;
    Ok(())
}

/// Records an accepted past operator action with what it returned.
pub fn operator_action(
    world: &World,
    state: &mut State,
    at: Timestamp,
    by: OperatorId,
    action: OperatorAction,
    outcome: ActionOutcome,
) -> Result<(), GenError> {
    operator_call(world, state, at, by, action, &Ok(outcome))
}

fn note(text: &str) -> Option<String> {
    Some(text.to_owned())
}

pub fn populate(world: &World, state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    policies(world, state, plan)?;
    agents(world, state)?;
    triage(world, state)?;
    verdicts(world, state)?;
    refused(world, state, plan)
}

/// The operator decisions in the channels' policy histories, and the
/// promotion, as the audit log recorded the actions that made them.
fn policies(world: &World, state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    for (key, decision) in decisions() {
        let PolicyAuthor::Operator(by) = decision.decision.by else {
            continue;
        };
        let action = OperatorAction::SetPolicy {
            channel: plan.id(key)?,
            policy: decision.kind,
            note: decision.decision.note.clone(),
        };
        operator_action(
            world,
            state,
            decision.decision.at,
            by,
            action,
            ActionOutcome::Applied,
        )?;
    }
    let (key, promotion) = team_notes_promotion();
    let channel = plan.id(key)?;
    // What the promotion superseded, as the registry stored it.
    let superseded = SupersededChannels::new(state.channels.values().filter_map(|record| {
        let stored = record.channel();
        let by = stored.origin.supersession()?.by;
        (by == channel).then_some(stored.id)
    }));
    let PolicyAuthor::Operator(by) = promotion.declaration().by else {
        return Err(GenError::Missing("the promotion's operator".to_owned()));
    };
    let promote = OperatorAction::PromoteChannel {
        channel,
        pattern: promotion.pattern().clone(),
        policy: promotion.decision().kind,
        note: promotion.decision().decision.note.clone(),
    };
    operator_action(
        world,
        state,
        promotion.at(),
        by,
        promote,
        ActionOutcome::ChannelPromoted {
            channel,
            superseded,
        },
    )?;
    Ok(())
}

fn agents(world: &World, state: &mut State) -> Result<(), GenError> {
    let merges = state.identity.merges().to_vec();
    for merge in &merges {
        if let MergeAuthor::Operator(by) = merge.by() {
            let request = MergeRequest::new(merge.source(), merge.target(), merge.by())
                .map_err(|e| GenError::invalid("MergeRequest", e))?;
            let action = OperatorAction::MergeAgents(request);
            operator_action(
                world,
                state,
                merge.at(),
                by,
                action,
                ActionOutcome::Merged(merge.id()),
            )?;
        }
        if let Some(reversal) = merge.reverted() {
            let action = OperatorAction::Unmerge { merge: merge.id() };
            operator_action(
                world,
                state,
                reversal.at,
                reversal.by,
                action,
                ActionOutcome::Applied,
            )?;
        }
    }
    let mut rng = Rng::fork(world.seed, "renames");
    let labelled: Vec<_> = state
        .identity
        .agents()
        .filter(|a| !world.scenario.cast.is_registered(a.id))
        .filter_map(|a| a.label.clone().map(|l| (a.id, l)))
        .collect();
    for (agent, label) in labelled {
        let at = plus(START, rng.below(5 * DAY));
        let action = OperatorAction::RenameAgent {
            agent,
            label: Some(label),
        };
        operator_action(
            world,
            state,
            at,
            OPERATOR_RESEARCHER,
            action,
            ActionOutcome::Applied,
        )?;
    }
    Ok(())
}

/// Audit entries for the acknowledgements and resolutions in the alert
/// history.
fn triage(world: &World, state: &mut State) -> Result<(), GenError> {
    let steps: Vec<(OperatorId, Timestamp, OperatorAction)> = state
        .alerts
        .iter()
        .filter_map(|a| match &a.state {
            AlertState::Acknowledged { by, at } => {
                Some((*by, *at, OperatorAction::Acknowledge { alert: a.id }))
            }
            AlertState::Resolved { by, at, note } => Some((
                *by,
                *at,
                OperatorAction::Resolve {
                    alert: a.id,
                    note: note.clone(),
                },
            )),
            _ => None,
        })
        .collect();
    for (by, at, action) in steps {
        operator_action(world, state, at, by, action, ActionOutcome::Applied)?;
    }
    Ok(())
}

fn verdicts(world: &World, state: &mut State) -> Result<(), GenError> {
    let mut rng = Rng::fork(world.seed, "verdicts");
    let mut log: Vec<(
        OperatorId,
        Timestamp,
        crosstalk_spec::ids::TransmissionId,
        Option<Verdict>,
        &str,
    )> = Vec::new();
    let mut withdrawn = false;
    for record in &world.transmissions {
        let t = &record.transmission;
        let age = NOW.as_micros().saturating_sub(t.opened_at.as_micros());
        if age < 2 * HOUR || !rng.chance(0.012) {
            continue;
        }
        let at = plus(t.opened_at, rng.between(HOUR, 20 * HOUR)).min(ago(30 * MINUTE));
        let by = if rng.chance(0.6) {
            OPERATOR_ONCALL
        } else {
            OPERATOR_RESEARCHER
        };
        let semantic = confirmed(&t.state).is_some_and(|c| {
            c.content()
                .iter()
                .any(|m| matches!(m.kind(), MatchKind::Semantic(_)))
        });
        let (verdict, text) = match &t.state {
            crosstalk_spec::derived::flow::transmission::TransmissionState::Suspected {
                ..
            } => (Verdict::FalseDetection, "unrelated read of the same page"),
            crosstalk_spec::derived::flow::transmission::TransmissionState::Discarded {
                ..
            } => (Verdict::Genuine, "content was paraphrased beyond matching"),
            _ if semantic && rng.chance(0.6) => {
                (Verdict::FalseDetection, "same topic, different text")
            }
            _ if record.is_confirmed() => (Verdict::Genuine, "checked the excerpts"),
            _ => continue,
        };
        log.push((by, at, t.id, Some(verdict), text));
        if !withdrawn && verdict == Verdict::Genuine && record.is_confirmed() {
            withdrawn = true;
            log.push((
                by,
                plus(at, 20 * MINUTE).min(ago(10 * MINUTE)),
                t.id,
                None,
                "judged the wrong row",
            ));
        }
    }
    for (by, at, transmission, verdict, text) in log {
        let record = world
            .tx(transmission)
            .ok_or_else(|| GenError::Missing(format!("judged transmission {transmission:?}")))?;
        let entry = TransmissionVerdict::new(&record.transmission, verdict, by, at, note(text))
            .map_err(|e| GenError::invalid("TransmissionVerdict", e))?;
        let recorded = crate::backend::fixture::actions::record_verdict(state, entry)
            .map_err(|e| GenError::invalid("VerdictLog", e))?;
        let outcome = if recorded == VerdictRecorded::Unchanged {
            ActionOutcome::Unchanged
        } else {
            ActionOutcome::Applied
        };
        operator_action(
            world,
            state,
            at,
            by,
            OperatorAction::SetVerdict {
                transmission,
                verdict,
                note: note(text),
            },
            outcome,
        )?;
        if recorded != VerdictRecorded::Unchanged && verdict == Some(Verdict::FalseDetection) {
            effects::reject_transmission_alerts(state, transmission, at);
        }
    }
    Ok(())
}

/// Two calls the log shows refused: a policy change by the on-call
/// operator, who lacks `Govern` (`Forbidden`), and the on-call operator
/// acknowledging an alert already resolved (`Conflict(AlertNotActive)`).
fn refused(world: &World, state: &mut State, plan: &ChannelPlan) -> Result<(), GenError> {
    let wiki = plan.id(ChannelKey::HijackedWiki)?;
    let action = OperatorAction::SetPolicy {
        channel: wiki,
        policy: PolicyKind::Sanctioned,
        note: note("looks like a normal wiki"),
    };
    let forbidden = Err(ActionError::Forbidden {
        missing: action.required_permission(),
    });
    operator_call(world, state, ago(DAY), OPERATOR_ONCALL, action, &forbidden)?;
    if let Some(alert) = state
        .alerts
        .iter()
        .find(|a| matches!(a.state, AlertState::Resolved { at, .. } if at < ago(DAY)))
        .map(|a| a.id)
    {
        let conflict = Err(ActionError::Conflict(ConflictKind::AlertNotActive {
            alert,
        }));
        operator_call(
            world,
            state,
            ago(20 * HOUR),
            OPERATOR_ONCALL,
            OperatorAction::Acknowledge { alert },
            &conflict,
        )?;
    }
    Ok(())
}
