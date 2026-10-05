//! What the topology tests share: a migrated test database, a fresh store
//! over it, and a small world (catalog, directories, node facts) with
//! 10 µs buckets.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::analysis::catalog::InMemoryTopicCatalog;
use crosstalk_memory::model::build::{agent, bucket_width, catalog, timing, transmission, ts};
use crosstalk_memory::model::topology::EdgeWorld;
use crosstalk_memory::support::Outbox;
use crosstalk_memory::topology::env::StaticNodes;
use crosstalk_memory::topology::store::EdgeStoreConfig as MemoryConfig;
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
use crosstalk_spec::derived::flow::transmission::{Classification, Route};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::events::insight::ClassificationCause;
use crosstalk_spec::ids::{AgentId, TransmissionId};
use crosstalk_spec::interfaces::l6_analysis::lifecycle::{
    CatalogActivation, StoredAssignment, TopicLifecycle, TopicLifecycleError,
};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest};
use crosstalk_spec::support::{Change, TimeWindow, Timestamp};
use crosstalk_store::TestDb;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::env::Env;
use crate::outbox::OutboxRelay;
use crate::store::partition::PartitionSpan;
use crate::store::{EdgeStoreConfig, PgEdgeStore, migrate};

pub const WIDTH: u64 = 10;
pub const SETTLE: u64 = 20;

pub type TestEnv = Env<InMemoryTopicCatalog, StaticDirectory, StaticNodes>;
pub type Store = PgEdgeStore<TestEnv>;

/// The store configuration the tests use: 10 µs buckets, `settle_after`
/// 20 µs, 1000 µs partitions (so a test spans several).
pub fn config() -> EdgeStoreConfig {
    EdgeStoreConfig {
        bucket_width: bucket_width(WIDTH),
        timing: timing(SETTLE).expect("a timing"),
        partition_span: PartitionSpan::from_micros(NonZeroU64::new(1000).expect("non-zero")),
    }
}

/// The store configuration matching a memory harness's.
pub fn config_from(memory: MemoryConfig) -> EdgeStoreConfig {
    EdgeStoreConfig {
        bucket_width: memory.bucket_width,
        timing: memory.timing,
        partition_span: PartitionSpan::from_micros(NonZeroU64::new(1000).expect("non-zero")),
    }
}

/// A fresh, migrated test database, or `None` (the test passes) without
/// `TEST_DATABASE_URL`.
pub async fn database(test: &str) -> Option<TestDb> {
    let db = TestDb::new_or_skip(test).await.expect("test database")?;
    migrate(db.pool()).await.expect("topology migrations");
    Some(db)
}

/// Empty every table and reset the control rows, as a fresh migration
/// leaves them.
pub async fn reset(pool: &PgPool) {
    sqlx::raw_sql(
        "TRUNCATE topology.contributions, topology.refit_processed, topology.edge_buckets, \
         topology.accesses, topology.access_buckets, topology.verdicts, topology.cursors, \
         topology.outbox, topology.versions; \
         UPDATE topology.state SET active_version = 0, watermark_micros = 0; \
         INSERT INTO topology.versions (version, activated) VALUES (0, true);",
    )
    .execute(pool)
    .await
    .expect("reset the topology tables");
}

/// A small pool on the test database, for a runtime of its own.
pub async fn small_pool(options: PgConnectOptions, size: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(size)
        .connect_with(options)
        .await
        .expect("a pool on the test database")
}

/// A world over `pool`: a fresh catalog (version 0 active, keeping three
/// versions), empty directories and node facts, and the store.
pub struct World {
    pub catalog: InMemoryTopicCatalog,
    pub directory: StaticDirectory,
    pub nodes: StaticNodes,
    pub store: Store,
    /// Kept so the store's wake-ups have a receiver, as in a deployment.
    pub _relay: OutboxRelay,
}

pub fn world(pool: PgPool) -> World {
    let catalog = catalog(3, 0.5, Outbox::none()).expect("a catalog");
    let directory = StaticDirectory::new();
    let nodes = StaticNodes::new();
    let env = Env {
        catalog: catalog.clone(),
        directory: directory.clone(),
        nodes: nodes.clone(),
    };
    let (store, relay) = PgEdgeStore::new(pool, config(), env);
    World {
        catalog,
        directory,
        nodes,
        store,
        _relay: relay,
    }
}

/// Transmission `n` from agent `from` to agent `to` at `at`, `bytes`
/// matched, classified under `version` (no topic), confirmed.
pub fn contribution(
    n: u64,
    from: u64,
    to: u64,
    route: Route,
    at: u64,
    bytes: u64,
) -> EdgeContribution {
    EdgeContribution {
        transmission: transmission(n),
        from: agent(from),
        to: agent(to),
        route,
        at: ts(at),
        matched_bytes: NonZeroU64::new(bytes).expect("non-zero bytes"),
        classification: Classification {
            version: TopicModelVersion(0),
            topic: None,
            watched: false,
        },
        cause: ClassificationCause::Confirmation,
    }
}

pub fn window(start: u64, end: u64) -> TimeWindow {
    TimeWindow::new(ts(start), ts(end)).expect("a non-empty window")
}

pub fn frontier(ticked: u64) -> PipelineFrontier {
    PipelineFrontier {
        ticked_through: ts(ticked),
        oldest_pending: None,
    }
}

/// A graph's edges as (from, to, transmissions, bytes), sorted.
pub fn edge_counts(graph: &TopologyGraph) -> Vec<(AgentId, AgentId, u64, u64)> {
    let mut edges: Vec<_> = graph
        .edges()
        .iter()
        .map(|edge| {
            (
                edge.from,
                edge.to,
                edge.stats.transmissions.get(),
                edge.stats.matched_bytes.get(),
            )
        })
        .collect();
    edges.sort();
    edges
}

pub async fn graph(store: &Store, window: TimeWindow, filter: &TopologyFilter) -> TopologyGraph {
    store
        .graph(window, Weighting::Transmissions, filter)
        .await
        .expect("a graph")
        .value
}

/// What the model harness drives: the Postgres store, and the catalog the
/// store reads, through their spec traits.
pub struct PgSubject {
    pub store: Store,
    pub catalog: InMemoryTopicCatalog,
}

impl PgSubject {
    /// A fresh subject over `pool` (emptied first) and the harness's world.
    pub async fn new(pool: PgPool, config: MemoryConfig, world: EdgeWorld) -> Self {
        reset(&pool).await;
        let reference =
            crosstalk_memory::model::topology::ReferenceEdges::new(config, world.clone())
                .expect("a reference world");
        let env = Env {
            catalog: reference.catalog.clone(),
            directory: world.directory,
            nodes: world.nodes,
        };
        let (store, _relay) = PgEdgeStore::new(pool, config_from(config), env);
        Self {
            store,
            catalog: reference.catalog,
        }
    }
}

impl TopicLifecycle for PgSubject {
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

impl EdgeStore for PgSubject {
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
