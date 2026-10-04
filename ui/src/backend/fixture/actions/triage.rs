//! Verdicts, alert triage and dead-letter replay (item 17).

use crosstalk_spec::aggregates::alert::AlertState;
use crosstalk_spec::derived::flow::transmission::TransmissionState;
use crosstalk_spec::ids::{AlertId, EventId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::World;
use crate::contract::actions::ActionOutcome;
use crate::contract::errors::{ConflictKind, QueryError};
use crate::contract::verdict::{TransmissionVerdict, Verdict};

use super::effects;

/// Allowed on `Suspected`, `Discarded` and every state holding a
/// `Confirmed`. A false detection resolves the transmission's active
/// alerts.
pub fn set_verdict(
    world: &World,
    state: &mut State,
    by: OperatorId,
    transmission: TransmissionId,
    verdict: Option<Verdict>,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let record = world.tx(transmission).ok_or(QueryError::NotFound)?;
    match record.transmission.state {
        TransmissionState::Suspected { .. }
        | TransmissionState::Discarded { .. }
        | TransmissionState::Confirmed(_)
        | TransmissionState::Classified { .. }
        | TransmissionState::Aggregated { .. } => {}
        TransmissionState::Detected | TransmissionState::AwaitingContent { .. } => {
            return Err(QueryError::Conflict(ConflictKind::NotJudgeable));
        }
    }
    state.verdicts.push(TransmissionVerdict {
        transmission,
        verdict,
        by,
        at: NOW,
        note,
    });
    if verdict == Some(Verdict::FalseDetection) {
        effects::reject_transmission_alerts(state, transmission, by, NOW);
    }
    Ok(ActionOutcome::Applied)
}

pub fn acknowledge(state: &mut State, by: OperatorId, alert: AlertId) -> Result<ActionOutcome> {
    let alert = state
        .alerts
        .iter_mut()
        .find(|a| a.id == alert)
        .ok_or(QueryError::NotFound)?;
    if alert.state != AlertState::Open {
        return Err(QueryError::Conflict(ConflictKind::AlertState));
    }
    alert.state = AlertState::Acknowledged { by, at: NOW };
    Ok(ActionOutcome::Applied)
}

pub fn resolve(
    state: &mut State,
    by: OperatorId,
    alert: AlertId,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let alert = state
        .alerts
        .iter_mut()
        .find(|a| a.id == alert)
        .ok_or(QueryError::NotFound)?;
    if !effects::is_active(alert) {
        return Err(QueryError::Conflict(ConflictKind::AlertState));
    }
    alert.state = AlertState::Resolved { by, at: NOW, note };
    Ok(ActionOutcome::Applied)
}

/// Redelivers a dead letter, which removes it from the store.
pub fn replay(state: &mut State, group: &ConsumerGroup, id: EventId) -> Result<ActionOutcome> {
    let index = state
        .dead_letters
        .iter()
        .position(|l| l.group == *group && l.envelope.id == id)
        .ok_or(QueryError::NotFound)?;
    state.dead_letters.remove(index);
    Ok(ActionOutcome::Applied)
}
