//! [`SpecExportSource`]: an `ExportSource` built from the spec's read
//! traits alone, for the datasets those traits can produce.
//!
//! An `ExportSource` is meant to read every store in one database snapshot;
//! a deployment on Postgres implements it there. Until then this source
//! serves the in-process surface:
//!
//! | Dataset | Rows from |
//! | --- | --- |
//! | projection | `ProjectionStore::projection`, `projection_rows` of its frame; labels from `TopicCatalog::topics` |
//! | accesses | `EdgeStore::channel_topology` of each bucket of the settled window |
//! | edges | `EdgeStore::graph` of each bucket, once per topic of the version (one-topic filters), outliers as the rest |
//! | topics | `EdgeStore::totals` of the settled window under a one-topic filter, per topic |
//! | transmissions, verdicts | refused: no spec trait lists the transmissions of a window |
//!
//! Every row is read when the export is planned (the count must be known
//! first anyway), so a store change while it streams changes nothing sent.
//! The topics dataset needs a bucket-aligned window here, as `totals` does.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroU64;

use crosstalk_spec::aggregates::edge::{
    EdgeSelector, EdgeTotals, TopologyFilter, TopologyGraph, Weighting,
};
use crosstalk_spec::aggregates::filter::VersionUnavailable;
use crosstalk_spec::aggregates::projection::ProjectionStatus;
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::TopicVersionHistory;
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::ids::{AgentId, ProjectionId, TopicId};
use crosstalk_spec::interfaces::l6_analysis::{
    CatalogError, Embedder, ProjectionStore, TopicCatalog,
};
use crosstalk_spec::interfaces::l7_topology::{EdgeQueryError, EdgeStore};
use crosstalk_spec::interfaces::l8_surface::export::rows::{
    AccessRow, EdgeRow, LabelContent, TopicContent, TopicRow, projection_rows,
};
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportBasis, ExportDataset, ExportPlan, ExportPlanError, ExportRequest, ExportRow, ExportScope,
    ExportSource, RowSource, SourceFailure, settled_window,
};
use crosstalk_spec::paging::{PageRequest, PageSize};
use crosstalk_spec::support::{TimeWindow, Timestamp, Watermark};

/// Rows read when the export was planned, handed out in key order.
#[derive(Debug, Default)]
pub struct PlannedRows(VecDeque<ExportRow>);

impl PlannedRows {
    /// `rows` in ascending key order, as the sealer requires.
    pub fn new(mut rows: Vec<ExportRow>) -> Self {
        rows.sort_by_key(ExportRow::key);
        Self(rows.into())
    }
}

impl RowSource for PlannedRows {
    async fn next(&mut self) -> Result<Option<ExportRow>, SourceFailure> {
        Ok(self.0.pop_front())
    }
}

/// An `ExportSource` over the spec's read traits (module docs).
#[derive(Debug, Clone)]
pub struct SpecExportSource<E, P, T, M> {
    edges: E,
    projections: P,
    topics: T,
    embedder: M,
}

fn store(reason: impl Into<String>) -> ExportPlanError {
    ExportPlanError::Store {
        reason: reason.into(),
    }
}

fn edge_error(error: EdgeQueryError) -> ExportPlanError {
    match error {
        EdgeQueryError::Store { reason } => ExportPlanError::Store { reason },
        EdgeQueryError::UnalignedWindow | EdgeQueryError::BucketWidthMismatch { .. } => {
            ExportPlanError::UnalignedWindow
        }
        EdgeQueryError::Version(version) => ExportPlanError::Version(version),
        EdgeQueryError::TopicsNotInVersion { version, topics } => {
            ExportPlanError::TopicsNotInVersion { version, topics }
        }
        EdgeQueryError::InvalidCursor => store("edge store cursor error during an export plan"),
    }
}

fn catalog_error(error: CatalogError) -> ExportPlanError {
    match error {
        CatalogError::Store { reason } => ExportPlanError::Store { reason },
        CatalogError::UnknownVersion(version) => {
            ExportPlanError::Version(VersionUnavailable::Unknown(version))
        }
        CatalogError::StillFitting(version) => {
            ExportPlanError::Version(VersionUnavailable::Fitting(version))
        }
        CatalogError::VersionNotRetained(version) => {
            ExportPlanError::Version(VersionUnavailable::NotRetained(version))
        }
        CatalogError::InvalidCursor => store("catalog cursor error during an export plan"),
    }
}

/// The aligned buckets of `window`, oldest first.
fn buckets(window: TimeWindow, width: NonZeroU64) -> Vec<TimeWindow> {
    let width = width.get();
    let mut start = window.start().as_micros();
    let mut buckets = Vec::new();
    while start < window.end().as_micros() {
        let end = start.saturating_add(width).min(window.end().as_micros());
        if let Ok(bucket) =
            TimeWindow::new(Timestamp::from_micros(start), Timestamp::from_micros(end))
        {
            buckets.push(bucket);
        }
        start = end;
    }
    buckets
}

impl<E, P, T, M> SpecExportSource<E, P, T, M>
where
    E: EdgeStore + Send + Sync,
    P: ProjectionStore + Send + Sync,
    T: TopicCatalog + Send + Sync,
    M: Embedder + Send + Sync,
{
    pub fn new(edges: E, projections: P, topics: T, embedder: M) -> Self {
        Self {
            edges,
            projections,
            topics,
            embedder,
        }
    }

    async fn all_topics(&self, version: TopicModelVersion) -> Result<Vec<Topic>, ExportPlanError> {
        let size = PageSize::new(PageSize::MAX).map_err(|error| store(format!("{error:?}")))?;
        let mut request = PageRequest { size, after: None };
        let mut topics = Vec::new();
        loop {
            let page = self
                .topics
                .topics(version, &request)
                .await
                .map_err(catalog_error)?;
            let (items, next) = page.into_parts();
            topics.extend(items);
            match next {
                Some(next) => request.after = Some(next),
                None => return Ok(topics),
            }
        }
    }

    async fn projection(
        &self,
        request: &ExportRequest,
        id: ProjectionId,
    ) -> Result<(ExportBasis, Vec<ExportRow>), ExportPlanError> {
        let projection = self
            .projections
            .projection(id)
            .await
            .map_err(ExportPlanError::Projection)?;
        let ProjectionStatus::Ready(fitted) = projection.info().status().clone() else {
            return Err(store(format!("projection {id:?} served without a fit")));
        };
        let labels = if request.include_content() {
            let topics = self.all_topics(projection.topic_version()).await?;
            Some(
                topics
                    .into_iter()
                    .map(|topic| (topic.id, topic.label))
                    .collect::<HashMap<_, _>>(),
            )
        } else {
            None
        };
        let rows = projection_rows(&projection, labels.as_ref());
        let basis = ExportBasis::Projection {
            projection: id,
            spec: projection.info().spec().clone(),
            fitted,
        };
        Ok((basis, rows))
    }

    /// The scope's version, resolved as a linked view resolves it, with
    /// its topics and the filter pinned to it.
    async fn scoped(
        &self,
        scope: &ExportScope,
    ) -> Result<(TopicModelVersion, Vec<Topic>, TopologyFilter), ExportPlanError> {
        let history: TopicVersionHistory = self.topics.versions().await.map_err(catalog_error)?;
        let version = scope
            .filter
            .topic_version
            .resolve(&history, |version| {
                history
                    .get(version)
                    .is_some_and(|info| info.retention().is_retained())
            })
            .map_err(ExportPlanError::Version)?;
        let topics = self.all_topics(version).await?;
        let outside = scope.filter.topics_outside(version, |topic| {
            topics
                .iter()
                .any(|known| known.id == topic)
                .then_some(version)
        });
        if !outside.is_empty() {
            return Err(ExportPlanError::TopicsNotInVersion {
                version,
                topics: outside,
            });
        }
        Ok((version, topics, scope.filter.clone().pinned(version)))
    }

    fn aligned(&self, window: TimeWindow) -> Result<NonZeroU64, ExportPlanError> {
        let width = self.edges.bucket_width();
        if width.is_boundary(window.start()) && width.is_boundary(window.end()) {
            Ok(width.as_micros())
        } else {
            Err(ExportPlanError::UnalignedWindow)
        }
    }

    async fn accesses(
        &self,
        filter: &TopologyFilter,
        settled: Option<TimeWindow>,
        width: NonZeroU64,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        let mut rows = Vec::new();
        for bucket in settled
            .map(|window| buckets(window, width))
            .unwrap_or_default()
        {
            let graph = self
                .edges
                .channel_topology(bucket, Weighting::Transmissions, filter)
                .await
                .map_err(edge_error)?;
            rows.extend(graph.value.accesses().iter().map(|access| {
                ExportRow::Access(AccessRow {
                    agent: access.agent,
                    channel: access.channel,
                    op: access.op,
                    bucket,
                    accesses: access.accesses,
                })
            }));
        }
        Ok(rows)
    }

    async fn graph(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> Result<TopologyGraph, ExportPlanError> {
        Ok(self
            .edges
            .graph(window, Weighting::Transmissions, filter)
            .await
            .map_err(edge_error)?
            .value)
    }

    async fn edges_rows(
        &self,
        filter: &TopologyFilter,
        topics: &[Topic],
        settled: Option<TimeWindow>,
        width: NonZeroU64,
        content: bool,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        let labels: HashMap<TopicId, &str> = topics
            .iter()
            .map(|topic| (topic.id, topic.label.as_str()))
            .collect();
        let slots: Vec<TopicId> = if filter.topics.is_empty() {
            topics.iter().map(|topic| topic.id).collect()
        } else {
            filter.topics.clone()
        };
        let mut rows = Vec::new();
        for bucket in settled
            .map(|window| buckets(window, width))
            .unwrap_or_default()
        {
            // What each (from, to, route) carried in the bucket under the
            // whole filter, less what each topic carried: the outliers.
            let mut rest: BTreeMap<EdgeKey, Remainder> = BTreeMap::new();
            if filter.topics.is_empty() {
                for edge in self.graph(bucket, filter).await?.edges() {
                    rest.insert(
                        edge_key(edge.from, edge.to, &edge.route),
                        (
                            edge.route.clone(),
                            edge.stats.transmissions.get(),
                            edge.stats.matched_bytes.get(),
                        ),
                    );
                }
            }
            for &topic in &slots {
                let one = TopologyFilter {
                    topics: vec![topic],
                    ..filter.clone()
                };
                for edge in self.graph(bucket, &one).await?.edges() {
                    if let Some(left) = rest.get_mut(&edge_key(edge.from, edge.to, &edge.route)) {
                        left.1 = left.1.saturating_sub(edge.stats.transmissions.get());
                        left.2 = left.2.saturating_sub(edge.stats.matched_bytes.get());
                    }
                    let selector = EdgeSelector::new(edge.from, edge.to, edge.route.clone())
                        .map_err(|_| store("the graph returned a self-edge"))?;
                    rows.push(ExportRow::Edge(EdgeRow {
                        edge: selector,
                        topic: Some(topic),
                        bucket,
                        transmissions: edge.stats.transmissions,
                        matched_bytes: edge.stats.matched_bytes,
                        content: content.then(|| LabelContent {
                            topic_label: labels.get(&topic).map(|label| (*label).to_owned()),
                        }),
                    }));
                }
            }
            for ((from, to, _), (route, transmissions, bytes)) in rest {
                let (Some(transmissions), Some(matched_bytes)) =
                    (NonZeroU64::new(transmissions), NonZeroU64::new(bytes))
                else {
                    continue;
                };
                let selector = EdgeSelector::new(from, to, route)
                    .map_err(|_| store("the graph returned a self-edge"))?;
                rows.push(ExportRow::Edge(EdgeRow {
                    edge: selector,
                    topic: None,
                    bucket,
                    transmissions,
                    matched_bytes,
                    content: content.then_some(LabelContent { topic_label: None }),
                }));
            }
        }
        Ok(rows)
    }

    async fn topic_rows(
        &self,
        filter: &TopologyFilter,
        topics: Vec<Topic>,
        settled: Option<TimeWindow>,
        content: bool,
    ) -> Result<Vec<ExportRow>, ExportPlanError> {
        let mut rows = Vec::new();
        for topic in topics {
            if !filter.topics.is_empty() && !filter.topics.contains(&topic.id) {
                continue;
            }
            let totals = match settled {
                Some(window) => {
                    let one = TopologyFilter {
                        topics: vec![topic.id],
                        ..filter.clone()
                    };
                    self.edges
                        .totals(window, &one)
                        .await
                        .map_err(edge_error)?
                        .value
                }
                None => EdgeTotals {
                    topic_version: topic.version,
                    transmissions: 0,
                    matched_bytes: 0,
                    active_channels: 0,
                },
            };
            rows.push(ExportRow::Topic(TopicRow {
                topic: topic.id,
                transmissions: totals.transmissions,
                matched_bytes: totals.matched_bytes,
                content: content.then(|| TopicContent {
                    label: topic.label.clone(),
                    terms: topic.terms.clone(),
                }),
            }));
        }
        Ok(rows)
    }
}

fn scoped_basis(
    topic_version: TopicModelVersion,
    filter: TopologyFilter,
    settled: Option<TimeWindow>,
) -> ExportBasis {
    ExportBasis::Scoped {
        topic_version,
        filter,
        settled,
    }
}

/// An edge of one bucket's graph: sender, reader and the route's canonical
/// encoding.
type EdgeKey = (AgentId, AgentId, Vec<u8>);

/// What an edge carried that no topic's graph accounted for: its route,
/// transmissions and matched bytes.
type Remainder = (Route, u64, u64);

fn edge_key(from: AgentId, to: AgentId, route: &Route) -> EdgeKey {
    let mut bytes = Vec::new();
    crosstalk_spec::interfaces::l8_surface::export::digest::encode_route(route, &mut bytes);
    (from, to, bytes)
}

impl<E, P, T, M> ExportSource for SpecExportSource<E, P, T, M>
where
    E: EdgeStore + Send + Sync,
    P: ProjectionStore + Send + Sync,
    T: TopicCatalog + Send + Sync,
    M: Embedder + Send + Sync,
{
    type Rows = PlannedRows;

    async fn plan(
        &self,
        request: &ExportRequest,
        watermark: Watermark,
    ) -> Result<ExportPlan<PlannedRows>, ExportPlanError> {
        let content = request.include_content();
        let (basis, rows) = match request.dataset() {
            ExportDataset::Projection(id) => self.projection(request, *id).await?,
            ExportDataset::Accesses(scope) => {
                let width = self.aligned(scope.window)?;
                let (version, _, filter) = self.scoped(scope).await?;
                let settled = settled_window(scope.window, watermark);
                let rows = self.accesses(&filter, settled, width).await?;
                (scoped_basis(version, filter, settled), rows)
            }
            ExportDataset::Edges(scope) => {
                let width = self.aligned(scope.window)?;
                let (version, topics, filter) = self.scoped(scope).await?;
                let settled = settled_window(scope.window, watermark);
                let rows = self
                    .edges_rows(&filter, &topics, settled, width, content)
                    .await?;
                (scoped_basis(version, filter, settled), rows)
            }
            ExportDataset::Topics(scope) => {
                self.aligned(scope.window)?;
                let (version, topics, filter) = self.scoped(scope).await?;
                let settled = settled_window(scope.window, watermark);
                let rows = self.topic_rows(&filter, topics, settled, content).await?;
                (scoped_basis(version, filter, settled), rows)
            }
            ExportDataset::Transmissions(_) | ExportDataset::Verdicts(_) => {
                return Err(store(format!(
                    "a {:?} export needs a store that lists the transmissions of a window, \
                     which no spec read trait does",
                    request.dataset().kind()
                )));
            }
        };
        let count = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        Ok(ExportPlan {
            basis,
            embedding_model: self.embedder.model(),
            rows: count,
            source: PlannedRows::new(rows),
        })
    }
}
