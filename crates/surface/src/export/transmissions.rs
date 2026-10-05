//! Where [`SpecExportSource`](super::SpecExportSource) reads the
//! transmissions dataset from: a [`TransmissionSource`].
//!
//! - [`NoTransmissions`] refuses it, as the source did before the spec had
//!   `TransmissionStore::list`.
//! - [`StoredTransmissions`] lists every confirmed, classified or
//!   aggregated transmission (`TransmissionStore::list`) and keeps those
//!   whose `Confirmed::at` lies in the settled window, that cross agents
//!   and that the filter admits; each row is `TransmissionRow::of` under
//!   the store's directory, the current verdict and the topic under the
//!   header's version, so it equals the row `transmissions_by_id` lists.
//!   Content columns are not served: a request with them is refused.

use std::collections::BTreeSet;
use std::future::Future;

use crosstalk_spec::aggregates::filter::{FilterSubject, TopologyFilter};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aliases::Resolve;
use crosstalk_spec::derived::flow::transmission::{Crossing, Transmission, TransmissionState};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::transmissions::{TransmissionQuery, TransmissionStore};
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l8_surface::export::rows::TransmissionRow;
use crosstalk_spec::interfaces::l8_surface::export::{ExportPlanError, ExportRow};
use crosstalk_spec::interfaces::l8_surface::summary::{TopicUnder, TransmissionStateKind};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

fn store(reason: impl Into<String>) -> ExportPlanError {
    ExportPlanError::Store {
        reason: reason.into(),
    }
}

/// What the transmissions dataset reads.
pub trait TransmissionSource: Send + Sync {
    /// The rows of the confirmed transmissions in `settled` that `filter`
    /// admits, topics under `version`; `content` asks for content columns.
    fn rows(
        &self,
        filter: &TopologyFilter,
        version: TopicModelVersion,
        settled: Option<TimeWindow>,
        content: bool,
    ) -> impl Future<Output = Result<Vec<ExportRow>, ExportPlanError>> + Send;
}

/// No transmissions dataset: every request for it is refused.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoTransmissions;

impl TransmissionSource for NoTransmissions {
    async fn rows(
        &self,
        _filter: &TopologyFilter,
        _version: TopicModelVersion,
        _settled: Option<TimeWindow>,
        _content: bool,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        Err(store(
            "a Transmissions export needs a store that lists the transmissions of a window",
        ))
    }
}

/// The transmissions dataset from a transmission store and the
/// directories that resolve its ids.
#[derive(Debug, Clone)]
pub struct StoredTransmissions<S, D> {
    store: S,
    directory: D,
}

impl<S, D> StoredTransmissions<S, D> {
    pub fn new(store: S, directory: D) -> Self {
        Self { store, directory }
    }
}

/// The topic `transmission` has under `version`, as `transmissions_by_id`
/// reads it.
fn topic_under(transmission: &Transmission, version: TopicModelVersion) -> TopicUnder {
    match &transmission.state {
        TransmissionState::Classified { classification, .. }
        | TransmissionState::Aggregated { classification, .. }
            if classification.version == version =>
        {
            classification
                .topic
                .map_or(TopicUnder::Outlier, TopicUnder::Topic)
        }
        _ => TopicUnder::Unassigned,
    }
}

impl<S, D> StoredTransmissions<S, D>
where
    S: TransmissionStore + TransmissionVerdicts + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    /// Every stored confirmed (or later) transmission.
    async fn confirmed(&self) -> Result<Vec<Transmission>, ExportPlanError> {
        let window = TimeWindow::new(Timestamp::from_micros(0), crosstalk_spec::wire::time::MAX)
            .map_err(|_| store("the all-time window is empty"))?;
        let query = TransmissionQuery {
            window,
            states: Some(BTreeSet::from([
                TransmissionStateKind::Confirmed,
                TransmissionStateKind::Classified,
                TransmissionStateKind::Aggregated,
            ])),
            channel: None,
        };
        let size = PageSize::new(PageSize::MAX).map_err(|error| store(format!("{error:?}")))?;
        let mut request = PageRequest { size, after: None };
        let mut all = Vec::new();
        loop {
            let page = self
                .store
                .list(&query, &request)
                .await
                .map_err(|error| store(format!("listing transmissions: {error:?}")))?;
            let (items, next) = page.into_parts();
            all.extend(items);
            match next {
                Some(next) => request.after = Some(next),
                None => return Ok(all),
            }
        }
    }

    async fn verdict(
        &self,
        transmission: &Transmission,
    ) -> Result<Option<Verdict>, ExportPlanError> {
        if transmission.state.judgeable().is_err() {
            return Ok(None);
        }
        let log = self
            .store
            .log(transmission.id)
            .await
            .map_err(|error| store(format!("reading a verdict log: {error:?}")))?;
        Ok(log.current())
    }
}

impl<S, D> TransmissionSource for StoredTransmissions<S, D>
where
    S: TransmissionStore + TransmissionVerdicts + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
{
    async fn rows(
        &self,
        filter: &TopologyFilter,
        version: TopicModelVersion,
        settled: Option<TimeWindow>,
        content: bool,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        if content {
            return Err(store(
                "content columns of the transmissions dataset are not served by this source",
            ));
        }
        let Some(settled) = settled else {
            return Ok(Vec::new());
        };
        let directory = &self.directory;
        let aliases = Resolve {
            agents: move |id: AgentId| AgentDirectory::canonical(directory, id),
            channels: move |id: ChannelId| ChannelDirectory::canonical(directory, id),
        };
        let mut rows = Vec::new();
        for transmission in self.confirmed().await? {
            let Some(confirmed) = transmission.state.confirmed() else {
                continue;
            };
            if !settled.contains(confirmed.at())
                || transmission.crossing(aliases) == Crossing::WithinOneAgent
            {
                continue;
            }
            let verdict = self.verdict(&transmission).await?;
            let topic = topic_under(&transmission, version);
            let Ok(row) = TransmissionRow::of(&transmission, aliases, |_| verdict, |_| topic, None)
            else {
                continue;
            };
            let subject = FilterSubject {
                from: row.delivery().from,
                to: row.summary().to,
                route: &row.summary().route,
                topic: match topic {
                    TopicUnder::Topic(topic) => Some(topic),
                    TopicUnder::Outlier | TopicUnder::Unassigned => None,
                },
                false_detection: verdict == Some(Verdict::FalseDetection),
            };
            if filter.admits(&subject, aliases) {
                rows.push(ExportRow::Transmission(Box::new(row)));
            }
        }
        Ok(rows)
    }
}
