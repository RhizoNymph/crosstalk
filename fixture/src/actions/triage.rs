//! Verdicts, alert triage and dead-letter replay.

use crosstalk_spec::aggregates::alert::AlertState;
use crosstalk_spec::derived::flow::verdict::{
    TransmissionVerdict, Verdict, VerdictLog, VerdictRecorded,
};
use crosstalk_spec::ids::{AlertId, EventId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::interfaces::l8_surface::{ActionError, ActionOutcome, ConflictKind};

use crate::store::State;
use crate::world::World;

use super::{Acted, Stamp, effects};

/// `TransmissionVerdicts::set`, authored by the caller at the acceptance
/// time. Allowed on the states `TransmissionState::judgeable` accepts
/// (`TransmissionVerdict::new` refuses the others:
/// `Conflict(TransmissionNotJudgeable)`); an unknown transmission is
/// `NotFound`. The verdict already current appends nothing and is
/// `Unchanged`; an appended false detection suppresses the transmission's
/// active alerts (`OperatorRejected`). The spec maps `VerdictError` to a
/// `QueryError` only, so the action's refusals are built here as
/// `SetVerdict` documents them.
pub fn set_verdict(
    world: &World,
    state: &mut State,
    stamp: Stamp,
    transmission: TransmissionId,
    verdict: Option<Verdict>,
    note: Option<String>,
) -> Acted {
    let record = world.tx(transmission).ok_or(ActionError::NotFound)?;
    let entry = TransmissionVerdict::new(&record.transmission, verdict, stamp.by, stamp.at, note)
        .map_err(|_| {
        ActionError::Conflict(ConflictKind::TransmissionNotJudgeable { transmission })
    })?;
    if record_verdict(state, entry)? == VerdictRecorded::Unchanged {
        return Ok(ActionOutcome::Unchanged);
    }
    if verdict == Some(Verdict::FalseDetection) {
        effects::reject_transmission_alerts(state, transmission, stamp.at);
    }
    Ok(ActionOutcome::Applied)
}

/// Appends `entry` to its transmission's log ([`VerdictLog::record`]); a
/// log is stored once it holds a record.
pub fn record_verdict(
    state: &mut State,
    entry: TransmissionVerdict,
) -> Result<VerdictRecorded, ActionError> {
    let id = entry.transmission();
    let mut log = state
        .verdicts
        .remove(&id)
        .unwrap_or_else(|| VerdictLog::new(id));
    let recorded = log.record(entry).map_err(|e| ActionError::Store {
        reason: format!("verdict log of {id:?}: {e:?}"),
    });
    if log.revision().is_some() {
        state.verdicts.insert(id, log);
    }
    recorded
}

fn alert_mut(state: &mut State, id: AlertId) -> Result<&mut AlertState, ActionError> {
    state
        .alerts
        .iter_mut()
        .find(|a| a.id == id)
        .map(|a| &mut a.state)
        .ok_or(ActionError::NotFound)
}

/// The alert lifecycle's acknowledge: an open alert becomes acknowledged
/// by the caller; an acknowledged one already matches (`Unchanged`, keeping
/// who acknowledged it); a resolved or suppressed one is
/// `Conflict(AlertNotActive)`.
pub fn acknowledge(state: &mut State, stamp: Stamp, id: AlertId) -> Acted {
    let alert = alert_mut(state, id)?;
    match alert {
        AlertState::Open => {
            *alert = AlertState::Acknowledged {
                by: stamp.by,
                at: stamp.at,
            };
            Ok(ActionOutcome::Applied)
        }
        AlertState::Acknowledged { .. } => Ok(ActionOutcome::Unchanged),
        AlertState::Resolved { .. } | AlertState::Suppressed { .. } => {
            Err(ActionError::Conflict(ConflictKind::AlertNotActive {
                alert: id,
            }))
        }
    }
}

/// The alert lifecycle's resolve: an open or acknowledged alert becomes
/// resolved by the caller with the note; a resolved one already matches
/// (`Unchanged`, keeping who resolved it); a suppressed one is
/// `Conflict(AlertNotActive)`.
pub fn resolve(state: &mut State, stamp: Stamp, id: AlertId, note: Option<String>) -> Acted {
    let alert = alert_mut(state, id)?;
    match alert {
        AlertState::Open | AlertState::Acknowledged { .. } => {
            *alert = AlertState::Resolved {
                by: stamp.by,
                at: stamp.at,
                note,
            };
            Ok(ActionOutcome::Applied)
        }
        AlertState::Resolved { .. } => Ok(ActionOutcome::Unchanged),
        AlertState::Suppressed { .. } => Err(ActionError::Conflict(ConflictKind::AlertNotActive {
            alert: id,
        })),
    }
}

/// Redelivers a dead letter, which removes it from the store.
pub fn replay(state: &mut State, group: &ConsumerGroup, id: EventId) -> Acted {
    let index = state
        .dead_letters
        .iter()
        .position(|l| l.group == *group && l.envelope.id == id)
        .ok_or(ActionError::NotFound)?;
    state.dead_letters.remove(index);
    Ok(ActionOutcome::Applied)
}
