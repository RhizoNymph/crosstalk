//! Verdicts, alert triage and dead-letter replay (item 17).

use crosstalk_spec::derived::flow::verdict::{
    TransmissionVerdict, Verdict, VerdictLog, VerdictRecorded,
};
use crosstalk_spec::ids::{AlertId, EventId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;

use crate::backend::Result;
use crate::backend::fixture::clock::NOW;
use crate::backend::fixture::store::State;
use crate::backend::fixture::world::World;
use crate::contract::actions::ActionOutcome;
use crate::contract::alerts::AlertState;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryError};

use super::effects;

/// Allowed on the states `TransmissionState::judgeable` accepts
/// (`TransmissionVerdict::new` refuses the others). The verdict already
/// current appends nothing; an appended false detection suppresses the
/// transmission's active alerts (`OperatorRejected`).
pub fn set_verdict(
    world: &World,
    state: &mut State,
    by: OperatorId,
    transmission: TransmissionId,
    verdict: Option<Verdict>,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let record = world.tx(transmission).ok_or(QueryError::NotFound)?;
    let entry =
        TransmissionVerdict::new(&record.transmission, verdict, by, NOW, note).map_err(|_| {
            QueryError::Conflict(ConflictKind::TransmissionNotJudgeable { transmission })
        })?;
    if record_verdict(state, entry)? != VerdictRecorded::Unchanged
        && verdict == Some(Verdict::FalseDetection)
    {
        effects::reject_transmission_alerts(state, transmission, NOW);
    }
    Ok(ActionOutcome::Applied)
}

/// Appends `entry` to its transmission's log ([`VerdictLog::record`]); a
/// log is stored once it holds a record.
pub fn record_verdict(state: &mut State, entry: TransmissionVerdict) -> Result<VerdictRecorded> {
    let id = entry.transmission();
    let mut log = state
        .verdicts
        .remove(&id)
        .unwrap_or_else(|| VerdictLog::new(id));
    let recorded = log.record(entry).map_err(|e| QueryError::Store {
        reason: format!("verdict log of {id:?}: {e:?}"),
    });
    if log.revision().is_some() {
        state.verdicts.insert(id, log);
    }
    recorded
}

pub fn acknowledge(state: &mut State, by: OperatorId, id: AlertId) -> Result<ActionOutcome> {
    let alert = state
        .alerts
        .iter_mut()
        .find(|a| a.id == id)
        .ok_or(QueryError::NotFound)?;
    if alert.state != AlertState::Open {
        return Err(QueryError::Conflict(ConflictKind::AlertNotActive {
            alert: id,
        }));
    }
    alert.state = AlertState::Acknowledged { by, at: NOW };
    Ok(ActionOutcome::Applied)
}

pub fn resolve(
    state: &mut State,
    by: OperatorId,
    id: AlertId,
    note: Option<String>,
) -> Result<ActionOutcome> {
    let alert = state
        .alerts
        .iter_mut()
        .find(|a| a.id == id)
        .ok_or(QueryError::NotFound)?;
    if !effects::is_active(alert) {
        return Err(QueryError::Conflict(ConflictKind::AlertNotActive {
            alert: id,
        }));
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
