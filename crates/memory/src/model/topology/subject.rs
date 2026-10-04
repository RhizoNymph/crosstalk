//! What `check_edge_store` drives: the edge store and the topic catalog's
//! lifecycle (the versions it resolves selectors against), both through
//! their spec traits, and the world it reads at query time.

use std::collections::BTreeMap;

use crosstalk_spec::aggregates::access::{AccessEdge, BipartiteGraph};
use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmissionPage, TopologyFilter, TopologyGraph,
    Weighting,
};
use crosstalk_spec::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::{Topic, TopicModelVersion};
use crosstalk_spec::aggregates::topic_history::TopicLineage;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::ids::{AgentId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest};
use crosstalk_spec::support::{Change, TimeWindow, Timestamp};

use crate::analysis::aliases::StaticDirectory;
use crate::analysis::catalog::{CatalogConfig, InMemoryTopicCatalog, RetentionPolicy};
use crate::model::build::{bucket_width, similarity, timing, ts};
use crate::support::Outbox;
use crate::topology::env::{Env, StaticNodes};
use crate::topology::store::{EdgeStoreConfig, InMemoryEdgeStore};

/// Every spec trait the edge harness drives: the edge store, and the topic
/// catalog's lifecycle, whose history the store resolves selectors against.
pub trait EdgeSubject: EdgeStore + TopicLifecycle {}

impl<T: EdgeStore + TopicLifecycle> EdgeSubject for T {}

/// What the edge store reads from other layers at query time: the merges
/// and supersessions it resolves ids through (`AgentDirectory`,
/// `ChannelDirectory`) and the node facts its graphs describe nodes with
/// (`NodeFacts`). The harness changes both, for the subject and the
/// reference alike.
#[derive(Debug, Clone, Default)]
pub struct EdgeWorld {
    pub directory: StaticDirectory,
    pub nodes: StaticNodes,
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

/// The reference store and the catalog it reads.
#[derive(Clone)]
pub struct ReferenceEdges {
    pub catalog: InMemoryTopicCatalog,
    pub store: InMemoryEdgeStore<RefEnv>,
}

impl ReferenceEdges {
    /// A reference store over `world`, its catalog holding only version 0
    /// and keeping the three most recent activated versions.
    pub fn new(config: EdgeStoreConfig, world: EdgeWorld) -> Result<Self, String> {
        let catalog_config = CatalogConfig {
            retention: RetentionPolicy::new(3).map_err(|error| format!("{error:?}"))?,
            lineage_floor: similarity(0.5).ok_or("floor")?,
        };
        let catalog = InMemoryTopicCatalog::new(catalog_config, ts(0), Outbox::none())
            .map_err(|error| format!("{error:?}"))?;
        let env = Env {
            topics: catalog.clone(),
            directory: world.directory,
            nodes: world.nodes,
        };
        Ok(Self {
            catalog,
            store: InMemoryEdgeStore::new(config, env, Outbox::none()),
        })
    }
}

/// Fit a version with `topics` through `catalog` and make it ready, at `at`,
/// `at + 1` and `at + 2`.
pub async fn catalog_ready<C: TopicLifecycle>(
    catalog: &mut C,
    topics: Vec<Topic>,
    at: Timestamp,
) -> Result<TopicModelVersion, TopicLifecycleError> {
    let micros = at.as_micros();
    let version = catalog.begin_fit(at).await?;
    let fitted = ts(micros + 1);
    let topics = topics
        .into_iter()
        .map(|one| Topic {
            version,
            fitted_at: fitted,
            ..one
        })
        .collect();
    catalog.complete_fit(version, topics, fitted).await?;
    catalog.mark_ready(version, ts(micros + 2)).await?;
    Ok(version)
}

impl TopicLifecycle for ReferenceEdges {
    fn begin_fit(
        &mut self,
        at: Timestamp,
    ) -> impl Future<Output = Result<TopicModelVersion, TopicLifecycleError>> + Send {
        self.catalog.begin_fit(at)
    }

    fn complete_fit(
        &mut self,
        version: TopicModelVersion,
        topics: Vec<Topic>,
        fitted_at: Timestamp,
    ) -> impl Future<Output = Result<TopicLineage, TopicLifecycleError>> + Send {
        self.catalog.complete_fit(version, topics, fitted_at)
    }

    fn fail_fit(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<(), TopicLifecycleError>> + Send {
        self.catalog.fail_fit(version)
    }

    fn mark_ready(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<(), TopicLifecycleError>> + Send {
        self.catalog.mark_ready(version, at)
    }

    fn mark_active(
        &mut self,
        version: TopicModelVersion,
        at: Timestamp,
    ) -> impl Future<Output = Result<CatalogActivation, TopicLifecycleError>> + Send {
        self.catalog.mark_active(version, at)
    }

    fn assign(
        &mut self,
        transmission: TransmissionId,
        version: TopicModelVersion,
        assignment: StoredAssignment,
    ) -> impl Future<Output = Result<Change, TopicLifecycleError>> + Send {
        self.catalog.assign(transmission, version, assignment)
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

    fn version_ready(
        &mut self,
        version: TopicModelVersion,
        transmissions: u64,
    ) -> impl Future<Output = Result<(), EdgeError>> + Send {
        self.store.version_ready(version, transmissions)
    }

    fn activate(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<Activation, EdgeError>> + Send {
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
