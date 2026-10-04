//! The in-memory verdict store: `TransmissionVerdicts` over the stored
//! transmissions and one `VerdictLog` beside each.
//!
//! L5 keeps each transmission's log beside the transmission, so the
//! judgeable check and the append read one row. Here both live in one
//! table behind one lock, so a `set` reads the state and appends in one
//! critical section, and a state change racing it lands wholly before or
//! wholly after.
//!
//! Transmissions are written by the flow consumer, which the spec gives no
//! trait; [`SeedTransmissions::put`] is that write.

pub mod model;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::{
    InvalidVerdictRecord, TransmissionVerdict, Verdict, VerdictLog, VerdictRecorded,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::pipeline::{Outbox, State};

/// The flow consumer's write of a transmission, implemented by every
/// verdict store the model-based harness checks.
pub trait SeedTransmissions {
    /// Store `transmission`, replacing the stored one with its id (the
    /// consumer applies each state change this way). Its verdict log is
    /// kept. Announces nothing: transmission events are the consumer's.
    fn put(&mut self, transmission: Transmission) -> impl Future<Output = ()> + Send;
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct VerdictTable {
    pub(crate) transmissions: BTreeMap<TransmissionId, Transmission>,
    pub(crate) logs: BTreeMap<TransmissionId, VerdictLog>,
}

impl VerdictTable {
    fn set(
        &mut self,
        id: TransmissionId,
        verdict: Option<Verdict>,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<(VerdictRecorded, Vec<BusEvent>), VerdictError> {
        let transmission = self
            .transmissions
            .get(&id)
            .ok_or(VerdictError::UnknownTransmission(id))?;
        let record = TransmissionVerdict::new(transmission, verdict, by, at, note)
            .map_err(|_| VerdictError::NotJudgeable(id))?;
        let log = self.logs.entry(id).or_insert_with(|| VerdictLog::new(id));
        let recorded = log.record(record).map_err(|error| match error {
            InvalidVerdictRecord::OtherTransmission | InvalidVerdictRecord::RevisionsExhausted => {
                VerdictError::Store {
                    reason: format!("verdict log refused the record: {error:?}"),
                }
            }
        })?;
        let events = match recorded {
            VerdictRecorded::Unchanged => Vec::new(),
            VerdictRecorded::Appended(revision) => vec![
                BusEvent::Detect(DetectEvent::VerdictSet {
                    transmission: id,
                    verdict,
                    revision,
                    by,
                    at,
                }),
                BusEvent::Changed(Changed::Verdict(id)),
            ],
        };
        Ok((recorded, events))
    }

    fn log(&self, id: TransmissionId) -> Result<VerdictLog, VerdictError> {
        if !self.transmissions.contains_key(&id) {
            return Err(VerdictError::UnknownTransmission(id));
        }
        Ok(self
            .logs
            .get(&id)
            .cloned()
            .unwrap_or_else(|| VerdictLog::new(id)))
    }

    fn quality(&self, window: TimeWindow) -> DetectionQuality {
        DetectionQuality::tally(
            window,
            self.transmissions.values().map(|transmission| {
                let current = self
                    .logs
                    .get(&transmission.id)
                    .and_then(VerdictLog::current);
                (transmission, current)
            }),
        )
    }
}

/// The in-memory `TransmissionVerdicts`. Clones are handles on one store.
#[derive(Debug, Clone, Default)]
pub struct MemoryVerdicts {
    state: State<VerdictTable>,
    outbox: Outbox,
}

impl MemoryVerdicts {
    pub fn new(outbox: Outbox) -> Self {
        Self {
            state: State::new(VerdictTable::default()),
            outbox,
        }
    }

    /// The stored transmission, as the flow consumer last put it.
    pub fn transmission(&self, id: TransmissionId) -> Option<Transmission> {
        self.state.read().transmissions.get(&id).cloned()
    }
}

impl TransmissionVerdicts for MemoryVerdicts {
    async fn set(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        by: OperatorId,
        at: Timestamp,
        note: Option<String>,
    ) -> Result<VerdictRecorded, VerdictError> {
        let (recorded, events) = self
            .state
            .write()
            .set(transmission, verdict, by, at, note)?;
        self.outbox.publish(events);
        Ok(recorded)
    }

    /// `UnknownTransmission` for an id with no stored transmission; an
    /// empty log for a stored one never judged.
    async fn log(&self, transmission: TransmissionId) -> Result<VerdictLog, VerdictError> {
        self.state.read().log(transmission)
    }

    async fn quality(&self, window: TimeWindow) -> Result<DetectionQuality, VerdictError> {
        Ok(self.state.read().quality(window))
    }
}

impl SeedTransmissions for MemoryVerdicts {
    async fn put(&mut self, transmission: Transmission) {
        self.state
            .write()
            .transmissions
            .insert(transmission.id, transmission);
    }
}
