//! A snapshot of what a seeded world's stores hold, read once through the
//! spec's store read traits, and searches over it.

use std::collections::HashMap;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aliases::Resolve;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::flow::transmission::{Crossing, Transmission};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AccessId, AgentId, ResourceId, TransmissionId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::channels::AccessStore;
use crosstalk_spec::interfaces::l5_flow::transmissions::{TransmissionQuery, TransmissionStore};
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::harness::ProvisionError;

/// Every stored transmission with its verdicts and the resources its
/// co-access records touched, and the world's settling point.
pub struct Snapshot<'a, A, C> {
    agents: &'a A,
    channels: &'a C,
    /// Newest id first.
    pub transmissions: Vec<Transmission>,
    verdicts: HashMap<TransmissionId, Vec<Option<Verdict>>>,
    accesses: HashMap<AccessId, Resource>,
    watermark: Timestamp,
    bucket: BucketWidth,
    scenario: &'static str,
}

impl<'a, A, C> Snapshot<'a, A, C>
where
    A: AgentDirectory,
    C: ChannelDirectory + AccessStore,
{
    /// Reads every transmission `transmissions` stores, its verdict log and
    /// the resources of its co-access records.
    pub async fn read<T>(
        agents: &'a A,
        channels: &'a C,
        transmissions: &T,
        watermark: Timestamp,
        bucket: BucketWidth,
        scenario: &'static str,
    ) -> Result<Snapshot<'a, A, C>, ProvisionError>
    where
        T: TransmissionStore + TransmissionVerdicts,
    {
        let failed = |what: &str, detail: String| ProvisionError::Failed {
            scenario,
            reason: format!("reading {what}: {detail}"),
        };
        let everything =
            TimeWindow::new(Timestamp::from_micros(0), Timestamp::from_micros(u64::MAX))
                .map_err(|_| failed("transmissions", "an empty window".to_owned()))?;
        let query = TransmissionQuery {
            window: everything,
            states: None,
            channel: None,
        };
        let size =
            PageSize::new(PageSize::MAX).map_err(|e| failed("transmissions", format!("{e:?}")))?;
        let mut request = PageRequest { size, after: None };
        let mut stored = Vec::new();
        loop {
            let page = transmissions
                .list(&query, &request)
                .await
                .map_err(|e| failed("transmissions", format!("{e:?}")))?;
            let (items, next) = page.into_parts();
            stored.extend(items);
            match next {
                Some(cursor) => request.after = Some(cursor),
                None => break,
            }
        }
        let mut verdicts = HashMap::new();
        for transmission in &stored {
            let log = transmissions
                .log(transmission.id)
                .await
                .map_err(|e| failed("verdicts", format!("{e:?}")))?;
            let recorded: Vec<_> = log.records().iter().map(|r| r.verdict()).collect();
            if !recorded.is_empty() {
                verdicts.insert(transmission.id, recorded);
            }
        }
        let ids: Vec<AccessId> = stored
            .iter()
            .flat_map(|t| t.state.co_accesses())
            .flat_map(|co| [co.write(), co.read()])
            .collect();
        let mut accesses = HashMap::new();
        for chunk in ids.chunks(IdBatch::<AccessId>::MAX) {
            let batch = IdBatch::new(chunk.iter().copied())
                .map_err(|e| failed("accesses", format!("{e:?}")))?;
            let read = channels
                .accesses(&batch)
                .await
                .map_err(|e| failed("accesses", format!("{e:?}")))?;
            accesses.extend(read.into_iter().map(|(id, (_, resource))| (id, resource)));
        }
        let mut transmissions = stored;
        transmissions.sort_by_key(|t| std::cmp::Reverse(t.id));
        Ok(Snapshot {
            agents,
            channels,
            transmissions,
            verdicts,
            accesses,
            watermark,
            bucket,
            scenario,
        })
    }

    /// A binding failure naming what was missing.
    pub fn missing(&self, what: &str) -> ProvisionError {
        ProvisionError::Failed {
            scenario: self.scenario,
            reason: format!("the seeded world has no {what}"),
        }
    }

    pub fn canonical(&self, id: AgentId) -> AgentId {
        self.agents.canonical(id)
    }

    pub fn is_canonical(&self, id: AgentId) -> bool {
        self.canonical(id) == id
    }

    /// The writer a transmission's evidence names: its sender once
    /// confirmed, else the writer of its first co-access.
    pub fn writer(&self, t: &Transmission) -> Option<AgentId> {
        t.state
            .confirmed()
            .map(|c| c.from())
            .or_else(|| t.state.co_accesses().first().map(|co| co.writer()))
    }

    /// Whether a transmission crosses between two agents now.
    pub fn crosses(&self, t: &Transmission) -> bool {
        let aliases = Resolve {
            agents: |id| self.agents.canonical(id),
            channels: |id| self.channels.canonical(id),
        };
        t.crossing(aliases) == Crossing::Crosses
    }

    /// The verdicts recorded for `id`, oldest first.
    pub fn verdicts(&self, id: TransmissionId) -> &[Option<Verdict>] {
        self.verdicts.get(&id).map_or(&[], Vec::as_slice)
    }

    /// The resources a transmission's co-access records touched.
    pub fn resources(&self, t: &Transmission) -> impl Iterator<Item = &Resource> {
        t.state
            .co_accesses()
            .into_iter()
            .flat_map(|co| [co.write(), co.read()])
            .filter_map(|id| self.accesses.get(&id))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Whether a transmission's co-access records touched `resource`.
    pub fn touches(&self, t: &Transmission, resource: ResourceId) -> bool {
        self.resources(t).any(|r| r.id == resource)
    }

    /// Whether it was confirmed in a later bucket than it opened in.
    pub fn confirmed_later(&self, t: &Transmission) -> bool {
        let width = self.bucket.as_micros().get();
        t.state
            .confirmed()
            .is_some_and(|c| c.at().as_micros() / width > t.opened_at.as_micros() / width)
    }

    /// The newest transmission `keep` admits whose reader and writer are
    /// both canonical.
    pub fn tx(
        &self,
        what: &str,
        keep: impl Fn(&Transmission) -> bool,
    ) -> Result<&Transmission, ProvisionError> {
        self.transmissions
            .iter()
            .filter(|t| self.is_canonical(t.to))
            .filter(|t| self.writer(t).is_none_or(|w| self.is_canonical(w)))
            .find(|t| keep(t))
            .ok_or_else(|| self.missing(what))
    }

    /// The newest confirmed cross-agent transmission between canonical
    /// agents, settled before the watermark, that `keep` admits.
    pub fn confirmed(
        &self,
        what: &str,
        keep: impl Fn(&Transmission) -> bool,
    ) -> Result<&Transmission, ProvisionError> {
        self.tx(what, |t| {
            t.state.confirmed().is_some_and(|c| c.at() < self.watermark)
                && self.crosses(t)
                && keep(t)
        })
    }
}
