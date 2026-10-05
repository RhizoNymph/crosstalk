//! Where [`SpecExportSource`](super::SpecExportSource) reads the
//! transmissions dataset from: a [`TransmissionSource`].
//!
//! - [`NoTransmissions`] refuses it, as the source did before the spec had
//!   `TransmissionStore::list`.
//! - [`StoredTransmissions`] lists every transmission in the scope's
//!   states (`TransmissionStore::list`; confirmed, classified and
//!   aggregated by default) and keeps those whose row time
//!   (`TransmissionRow::at`: `Confirmed::at`, or `opened_at` for an
//!   unconfirmed one) lies in the settled window, that cross agents and
//!   that the filter admits (an unconfirmed one tested with the writer of
//!   its first co-access as sender, no topic); each row is `TransmissionRow::of` under
//!   the store's directory, the current verdict and the topic under the
//!   header's version, read as `transmissions_by_id` reads it: the
//!   catalog's stored assignment under that version
//!   (`TopicCatalog::assignments`), else the stored classification, so it
//!   equals the row `transmissions_by_id` lists under that version, a
//!   re-fitted transmission included. The filter's topic test sees that
//!   same topic.
//!   Content columns are not served: a request with them is refused.
//!   The verdicts dataset is read from it too: every judgeable transmission
//!   (suspected, discarded, confirmed or later) whose `Transmission::opened_at`
//!   lies in the settled window contributes `verdict_rows` of its verdict log,
//!   one row per record; a transmission without a verdict has no rows.

use std::collections::BTreeSet;
use std::future::Future;

use crosstalk_spec::aggregates::filter::{FilterSubject, TopologyFilter};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aliases::Aliases;
use crosstalk_spec::aliases::Resolve;
use crosstalk_spec::derived::flow::transmission::{Crossing, Transmission};
use crosstalk_spec::derived::flow::verdict::Verdict;
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::interfaces::l5_flow::transmissions::{TransmissionQuery, TransmissionStore};
use crosstalk_spec::interfaces::l5_flow::verdicts::TransmissionVerdicts;
use crosstalk_spec::interfaces::l6_analysis::TopicCatalog;
use crosstalk_spec::interfaces::l8_surface::export::rows::{TransmissionRow, verdict_rows};
use crosstalk_spec::interfaces::l8_surface::export::{ExportPlanError, ExportRow, ExportStates};
use crosstalk_spec::interfaces::l8_surface::summary::{TopicUnder, TransmissionStateKind};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use super::source::catalog_error;
use crate::query::content::read_topics_under;

fn store(reason: impl Into<String>) -> ExportPlanError {
    ExportPlanError::Store {
        reason: reason.into(),
    }
}

/// What the transmissions dataset reads.
pub trait TransmissionSource: Send + Sync {
    /// The rows of the transmissions in `states` whose row time lies in
    /// `settled` and that `filter` admits, topics under `version`; `content`
    /// asks for content columns.
    fn rows(
        &self,
        filter: &TopologyFilter,
        version: TopicModelVersion,
        settled: Option<TimeWindow>,
        states: &ExportStates,
        content: bool,
    ) -> impl Future<Output = Result<Vec<ExportRow>, ExportPlanError>> + Send;

    /// The verdicts dataset's rows: every verdict record of the judgeable
    /// transmissions opened in `settled`.
    fn verdict_rows(
        &self,
        settled: Option<TimeWindow>,
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
        _states: &ExportStates,
        _content: bool,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        Err(store(
            "a Transmissions export needs a store that lists the transmissions of a window",
        ))
    }

    async fn verdict_rows(
        &self,
        _settled: Option<TimeWindow>,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        Err(store(
            "a Verdicts export needs a store that lists the transmissions of a window",
        ))
    }
}

/// The transmissions dataset from a transmission store, the directories
/// that resolve its ids and the topic catalog its rows' topics are read
/// from.
#[derive(Debug, Clone)]
pub struct StoredTransmissions<S, D, C> {
    store: S,
    directory: D,
    catalog: C,
}

impl<S, D, C> StoredTransmissions<S, D, C> {
    pub fn new(store: S, directory: D, catalog: C) -> Self {
        Self {
            store,
            directory,
            catalog,
        }
    }
}

impl<S, D, C> StoredTransmissions<S, D, C>
where
    S: TransmissionStore + TransmissionVerdicts + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    C: TopicCatalog + Send + Sync,
{
    /// Every stored transmission in `states`.
    async fn in_states(&self, states: &ExportStates) -> Result<Vec<Transmission>, ExportPlanError> {
        let window = TimeWindow::new(Timestamp::from_micros(0), crosstalk_spec::wire::time::MAX)
            .map_err(|_| store("the all-time window is empty"))?;
        self.list(TransmissionQuery {
            window,
            states: Some(states.iter().collect::<BTreeSet<_>>()),
            channel: None,
        })
        .await
    }

    /// Every stored transmission `query` keeps, all pages.
    async fn list(&self, query: TransmissionQuery) -> Result<Vec<Transmission>, ExportPlanError> {
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

impl<S, D, C> TransmissionSource for StoredTransmissions<S, D, C>
where
    S: TransmissionStore + TransmissionVerdicts + Send + Sync,
    D: AgentDirectory + ChannelDirectory + Send + Sync,
    C: TopicCatalog + Send + Sync,
{
    async fn rows(
        &self,
        filter: &TopologyFilter,
        version: TopicModelVersion,
        settled: Option<TimeWindow>,
        states: &ExportStates,
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
        let crossing: Vec<Transmission> = self
            .in_states(states)
            .await?
            .into_iter()
            .filter(|transmission| transmission.crossing(aliases) != Crossing::WithinOneAgent)
            .collect();
        let topics = read_topics_under(&self.catalog, version, &crossing)
            .await
            .map_err(catalog_error)?;
        let mut rows = Vec::new();
        for transmission in crossing {
            let verdict = self.verdict(&transmission).await?;
            let topic = topics.of(&transmission);
            let Ok(row) = TransmissionRow::of_in_scope(
                &transmission,
                aliases,
                |_| verdict,
                |_| topic,
                None,
                states,
            ) else {
                continue;
            };
            if !settled.contains(row.at()) {
                continue;
            }
            // The sender: the delivery's for a confirmed row, the writer of
            // the first co-access for an unconfirmed one.
            let from = match row.delivery() {
                Some(delivery) => delivery.from,
                None => match transmission.state.co_accesses().first() {
                    Some(co_access) => aliases.agent(co_access.writer()),
                    None => continue,
                },
            };
            let subject = FilterSubject {
                from,
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

    async fn verdict_rows(
        &self,
        settled: Option<TimeWindow>,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        let Some(settled) = settled else {
            return Ok(Vec::new());
        };
        let directory = &self.directory;
        let aliases = Resolve {
            agents: move |id: AgentId| AgentDirectory::canonical(directory, id),
            channels: move |id: ChannelId| ChannelDirectory::canonical(directory, id),
        };
        let judgeable = self
            .list(TransmissionQuery {
                window: settled,
                states: Some(BTreeSet::from([
                    TransmissionStateKind::Suspected,
                    TransmissionStateKind::Discarded,
                    TransmissionStateKind::Confirmed,
                    TransmissionStateKind::Classified,
                    TransmissionStateKind::Aggregated,
                ])),
                channel: None,
            })
            .await?;
        let mut rows = Vec::new();
        for transmission in judgeable {
            let log = self
                .store
                .log(transmission.id)
                .await
                .map_err(|error| store(format!("reading a verdict log: {error:?}")))?;
            rows.extend(
                verdict_rows(&transmission, &log, aliases)
                    .map_err(|error| store(format!("verdict rows: {error:?}")))?,
            );
        }
        Ok(rows)
    }
}
