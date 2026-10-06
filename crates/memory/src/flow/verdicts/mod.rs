//! The in-memory transmission store: `TransmissionStore` and
//! `TransmissionVerdicts` over the stored transmissions and one
//! `VerdictLog` beside each.
//!
//! L5 keeps each transmission's log beside the transmission, so the
//! judgeable check and the append read one row. Here both live in one
//! table behind one lock, so a `set` reads the state and appends in one
//! critical section, and a state change racing it lands wholly before or
//! wholly after.
//!
//! The flow consumer writes each transmission state through
//! `TransmissionStore::save`, which keeps the transmission's log.

pub mod model;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crosstalk_spec::aggregates::quality::DetectionQuality;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::flow::verdict::{
    InvalidVerdictRecord, TransmissionVerdict, Verdict, VerdictLog, VerdictRecorded,
};
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::ids::{AgentId, OperatorId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::transmissions::{
    MatchKey, TransmissionQuery, TransmissionStore, TransmissionStoreError,
};
use crosstalk_spec::interfaces::l5_flow::verdicts::{TransmissionVerdicts, VerdictError};
use crosstalk_spec::paging::{Page, PageRequest, TransmissionList};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::analysis::aliases::StaticDirectory;
use crate::support::{CursorBook, Outbox, State, lock, page_after};

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

    fn quality(
        &self,
        window: TimeWindow,
        agent: impl Fn(AgentId) -> AgentId + Copy,
    ) -> DetectionQuality {
        DetectionQuality::tally(
            window,
            self.transmissions.values().map(|transmission| {
                let current = self
                    .logs
                    .get(&transmission.id)
                    .and_then(VerdictLog::current);
                (transmission, current)
            }),
            agent,
        )
    }
}

/// The in-memory `TransmissionStore` and `TransmissionVerdicts`. Clones are
/// handles on one store. `quality` resolves agents through the directory it
/// was given (none merged by default), so a transmission whose agents have
/// since merged into one is not counted.
/// Issued `list` cursors: each bound to its query, resuming after an id.
type ListCursors = CursorBook<TransmissionQuery, TransmissionId>;

#[derive(Clone)]
pub struct MemoryVerdicts {
    state: State<VerdictTable>,
    outbox: Outbox,
    agents: Arc<dyn AgentDirectory + Send + Sync>,
    channels: Arc<dyn ChannelDirectory + Send + Sync>,
    cursors: Arc<Mutex<ListCursors>>,
}

impl Default for MemoryVerdicts {
    fn default() -> Self {
        Self::new(Outbox::default())
    }
}

impl std::fmt::Debug for MemoryVerdicts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryVerdicts")
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl MemoryVerdicts {
    /// A store with no agent merged.
    pub fn new(outbox: Outbox) -> Self {
        Self::with_agents(StaticDirectory::default(), outbox)
    }

    /// A store that resolves agents through `agents` at the read.
    pub fn with_agents(
        agents: impl AgentDirectory + Send + Sync + 'static,
        outbox: Outbox,
    ) -> Self {
        Self::with_directories(agents, StaticDirectory::default(), outbox)
    }

    /// A store resolving agents through `agents` (quality) and channels
    /// through `channels` (`list`'s channel filter).
    pub fn with_directories(
        agents: impl AgentDirectory + Send + Sync + 'static,
        channels: impl ChannelDirectory + Send + Sync + 'static,
        outbox: Outbox,
    ) -> Self {
        Self {
            state: State::new(VerdictTable::default()),
            outbox,
            agents: Arc::new(agents),
            channels: Arc::new(channels),
            cursors: Arc::new(Mutex::new(ListCursors::default())),
        }
    }

    /// The stored transmission's route, read synchronously by the reference
    /// alert store, which matches alerts on a transmission's route.
    pub(crate) fn route(
        &self,
        id: TransmissionId,
    ) -> Option<crosstalk_spec::derived::flow::transmission::Route> {
        self.state
            .read()
            .transmissions
            .get(&id)
            .map(|transmission| transmission.route.clone())
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
        let agents = &self.agents;
        Ok(self
            .state
            .read()
            .quality(window, |agent| agents.canonical(agent)))
    }
}

impl TransmissionStore for MemoryVerdicts {
    async fn save(&mut self, transmission: Transmission) -> Result<(), TransmissionStoreError> {
        self.state
            .write()
            .transmissions
            .insert(transmission.id, transmission);
        Ok(())
    }

    async fn transmission(
        &self,
        id: TransmissionId,
    ) -> Result<Option<Transmission>, TransmissionStoreError> {
        Ok(self.state.read().transmissions.get(&id).cloned())
    }

    async fn list(
        &self,
        query: &TransmissionQuery,
        page: &PageRequest<TransmissionList>,
    ) -> Result<Page<Transmission, TransmissionList>, TransmissionStoreError> {
        let mut cursors = lock(&self.cursors);
        let after = match &page.after {
            None => None,
            Some(cursor) => Some(
                cursors
                    .resolve(cursor, query)
                    .ok_or(TransmissionStoreError::InvalidCursor)?,
            ),
        };
        let channels = &self.channels;
        let rows: Vec<Transmission> = self
            .state
            .read()
            .transmissions
            .values()
            .rev()
            .filter(|transmission| after.is_none_or(|after| transmission.id < after))
            .filter(|transmission| query.matches(transmission, |id| channels.canonical(id)))
            .cloned()
            .collect();
        page_after(
            &mut cursors,
            rows,
            page.size,
            query.clone(),
            |transmission| transmission.id,
        )
        .map_err(|error| TransmissionStoreError::Store {
            reason: error.to_string(),
        })
    }

    /// Every stored transmission's content matches, keyed, kept where
    /// asked for. Identity (reader exchange, sender, route) makes each key
    /// held by at most one transmission; were two to hold one, the newest
    /// id would answer.
    async fn holding(
        &self,
        matches: &BTreeSet<MatchKey>,
    ) -> Result<BTreeMap<MatchKey, TransmissionId>, TransmissionStoreError> {
        let state = self.state.read();
        let mut held = BTreeMap::new();
        for transmission in state.transmissions.values() {
            let Some(confirmed) = transmission.state.confirmed() else {
                continue;
            };
            for content in confirmed.content().iter() {
                let key = MatchKey::of(content);
                if matches.contains(&key) {
                    held.insert(key, transmission.id);
                }
            }
        }
        Ok(held)
    }
}
