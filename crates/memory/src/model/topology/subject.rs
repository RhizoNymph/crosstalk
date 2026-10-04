//! What `check_edge_store` drives: the edge store, the writes the spec
//! leaves to the implementation, and the world it reads at query time.

use std::sync::Arc;

use crosstalk_spec::aggregates::access::{AccessEdge, BipartiteGraph};
use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmissionPage, TopologyFilter, TopologyGraph,
    Weighting,
};
use crosstalk_spec::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::{AgentId, ChannelId, TransmissionId};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest};
use crosstalk_spec::support::{TimeWindow, Timestamp};
use std::collections::BTreeMap;

use crate::analysis::aliases::{AliasError, StaticDirectory};
use crate::analysis::catalog::{
    Activated, CatalogConfig, InMemoryTopicCatalog, LifecycleError, RetentionPolicy,
};
use crate::analysis::support::{Clock, ManualClock};
use crate::model::build::{bucket_width, similarity, timing, ts};
use crate::topology::env::{Env, StaticNodes};
use crate::topology::store::{Activation, EdgeStoreConfig, InMemoryEdgeStore};

/// An edge store with its classification and activation writes, and the
/// world it resolves through.
pub trait EdgeSubject: EdgeStore {
    fn apply_classified(
        &self,
        contribution: &EdgeContribution,
        cause: ClassificationCause,
    ) -> impl Future<Output = Result<EdgeKey, EdgeError>> + Send;

    fn version_ready(
        &self,
        version: TopicModelVersion,
        transmissions: u64,
    ) -> impl Future<Output = ()> + Send;

    fn activate_if_complete(
        &self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<Activation, EdgeError>> + Send;

    fn merge(&self, from: AgentId, into: AgentId) -> Result<(), AliasError>;

    fn unmerge(&self, agent: AgentId);

    fn supersede(&self, channel: ChannelId, by: ChannelId) -> Result<(), AliasError>;

    fn set_parent(&self, agent: AgentId, parent: Option<AgentId>);

    /// Fit a version with `topics` and make it ready in the catalog.
    fn catalog_ready(
        &self,
        topics: Vec<Topic>,
        at: Timestamp,
    ) -> impl Future<Output = Result<TopicModelVersion, LifecycleError>> + Send;

    /// `TopicVersionActivated` reached the catalog.
    fn catalog_activated(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<Activated, LifecycleError>> + Send;
}

/// The harness's store configuration: 10 µs buckets, `settle_after` of
/// 20 µs.
pub fn edge_config() -> Option<EdgeStoreConfig> {
    Some(EdgeStoreConfig {
        bucket_width: bucket_width(10),
        timing: timing(20)?,
    })
}

type RefEnv = Env<InMemoryTopicCatalog, StaticDirectory, StaticNodes>;

/// The reference store and its world.
#[derive(Clone)]
pub struct ReferenceEdges {
    pub catalog: InMemoryTopicCatalog,
    pub directory: StaticDirectory,
    pub nodes: StaticNodes,
    pub store: InMemoryEdgeStore<RefEnv>,
}

impl ReferenceEdges {
    pub fn new(config: EdgeStoreConfig) -> Result<Self, String> {
        let catalog_config = CatalogConfig {
            retention: RetentionPolicy::new(3).map_err(|error| format!("{error:?}"))?,
            lineage_floor: similarity(0.5).ok_or("floor")?,
        };
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(ts(0)));
        let catalog = InMemoryTopicCatalog::new(catalog_config, clock, ts(0))
            .map_err(|error| error.to_string())?;
        let directory = StaticDirectory::new();
        let nodes = StaticNodes::new();
        let env = Env {
            topics: catalog.clone(),
            directory: directory.clone(),
            nodes: nodes.clone(),
        };
        Ok(Self {
            catalog,
            directory,
            nodes,
            store: InMemoryEdgeStore::new(config, env),
        })
    }
}

impl EdgeStore for ReferenceEdges {
    fn apply(
        &mut self,
        contribution: &EdgeContribution,
    ) -> impl Future<Output = Result<EdgeKey, EdgeError>> + Send {
        self.store.apply(contribution)
    }

    fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> impl Future<Output = Result<Observed, EdgeError>> + Send {
        self.store.judge(transmission, verdict, revision)
    }

    fn activate(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<(), EdgeError>> + Send {
        self.store.activate(version)
    }

    fn drop_version(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<(), EdgeError>> + Send {
        self.store.drop_version(version)
    }

    fn watermark(&self) -> impl Future<Output = Result<Watermark, EdgeQueryError>> + Send {
        self.store.watermark()
    }

    fn advance_watermark(
        &mut self,
        frontier: PipelineFrontier,
    ) -> impl Future<Output = Result<Option<Watermark>, EdgeError>> + Send {
        self.store.advance_watermark(frontier)
    }

    fn apply_access(
        &mut self,
        access: &AccessContribution,
    ) -> impl Future<Output = Result<AccessEdge, EdgeError>> + Send {
        self.store.apply_access(access)
    }

    fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologyGraph>, EdgeQueryError>> + Send {
        self.store.graph(window, weighting, filter)
    }

    fn totals(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<EdgeTotals>, EdgeQueryError>> + Send {
        self.store.totals(window, filter)
    }

    fn channel_topology(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<BipartiteGraph>, EdgeQueryError>> + Send {
        self.store.channel_topology(window, weighting, filter)
    }

    fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> impl Future<Output = Result<Watermarked<EdgeTransmissionPage>, EdgeQueryError>> + Send
    {
        self.store.transmissions(edge, window, filter, page)
    }

    fn agent_traffic(
        &self,
        window: TimeWindow,
        agents: &[AgentId],
    ) -> impl Future<Output = Result<Watermarked<BTreeMap<AgentId, AgentTraffic>>, EdgeQueryError>> + Send
    {
        self.store.agent_traffic(window, agents)
    }

    fn bucket_width(&self) -> BucketWidth {
        self.store.bucket_width()
    }

    fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologySeries>, EdgeQueryError>> + Send {
        self.store.series(grid, weighting, grouping, filter)
    }
}

impl EdgeSubject for ReferenceEdges {
    async fn apply_classified(
        &self,
        contribution: &EdgeContribution,
        cause: ClassificationCause,
    ) -> Result<EdgeKey, EdgeError> {
        self.store.apply_classified(contribution, cause)
    }

    async fn version_ready(&self, version: TopicModelVersion, transmissions: u64) {
        self.store.version_ready(version, transmissions);
    }

    async fn activate_if_complete(
        &self,
        version: TopicModelVersion,
    ) -> Result<Activation, EdgeError> {
        self.store.activate_if_complete(version)
    }

    fn merge(&self, from: AgentId, into: AgentId) -> Result<(), AliasError> {
        self.directory.merge(from, into)
    }

    fn unmerge(&self, agent: AgentId) {
        self.directory.unmerge(agent);
    }

    fn supersede(&self, channel: ChannelId, by: ChannelId) -> Result<(), AliasError> {
        self.directory.supersede(channel, by)
    }

    fn set_parent(&self, agent: AgentId, parent: Option<AgentId>) {
        self.nodes.set_parent(agent, parent);
    }

    async fn catalog_ready(
        &self,
        topics: Vec<Topic>,
        at: Timestamp,
    ) -> Result<TopicModelVersion, LifecycleError> {
        let micros = at.as_micros();
        let version = self.catalog.begin_fit(at)?;
        let fitted = ts(micros + 1);
        let topics = topics
            .into_iter()
            .map(|one| Topic {
                version,
                fitted_at: fitted,
                ..one
            })
            .collect();
        self.catalog.fit_returned(version, topics, fitted)?;
        self.catalog.ready(version, ts(micros + 2))?;
        Ok(version)
    }

    async fn catalog_activated(
        &self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> Result<Activated, LifecycleError> {
        self.catalog.activated(version, at)
    }
}
