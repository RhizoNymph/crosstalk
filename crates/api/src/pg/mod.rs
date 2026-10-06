//! The Postgres store bundle: [`PgStores`], every store the surface reads
//! over one pool, each layer's store in its own schema (roadmap P7.3,
//! `docs/features/postgres_stores.md`).
//!
//! ```text
//! PgStores::open(PgOpen { pool, bus, dead_letters, blobs, clock, secret, ids, settings })
//!   L1 PgExchanges            L3 PgAgents (merge table loaded), PgConversations
//!   L4 PgProvenanceStore      L5 PgChannelRegistry (supersessions loaded), PgTransmissionStore
//!   L6 PgTopicCatalog, PgSearchIndex, PgProjectionStore, PgAlertStore (outboxes flushed)
//!   L7 PgEdgeStore (+ its OutboxRelay, returned for the caller to run)
//!   L8 PgAuditLog, PgOperatorStore, PgSinkRegistry
//!   PgEvidence: spans from L4's SpanIndex, accesses and resources from L5's tables
//! ```
//!
//! - **Outboxes.** Every store that decides events stages them in its
//!   schema's outbox and relays them, after the commit, through a `BusSink`
//!   onto `bus`: an awaited publish under an envelope id stamped once per
//!   row (INV-1211 to INV-1214). The sinks mint those ids with ULID
//!   generators from [`PgIds`]: OS entropy in a deployment
//!   (`surface.ids.unique-across-restart`), a fixed seed in tests.
//! - **Cursor keys** are derived from the deployment secret, one label per
//!   store (`crosstalk.cursor.v1.<store>`), so cursors resolve after a
//!   restart (`surface.cursor.survives-restart`).
//! - **Caches** rebuilt at open: `PgAgents`' merge table and the registry's
//!   supersessions. The node facts ([`NodeCache`]) start empty; the host
//!   rebuilds them (`InProcess::host`).
//! - **Time** is the injected clock's, passed to every store that needs
//!   it; no store reads a clock itself.

mod evidence;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_analysis::alerts::{AlertStoreConfig, AlertStoreParts, FlowFacts, PgAlertStore};
use crosstalk_analysis::pg::{BusSink as AnalysisSink, CursorKey};
use crosstalk_analysis::projections::{PgProjectionStore, ProjectionParts, ProjectionStoreConfig};
use crosstalk_analysis::search::PgSearchIndex;
use crosstalk_analysis::topics::{CatalogConfig, CatalogParts, PgTopicCatalog};
use crosstalk_canonical::exchanges::PgExchanges;
use crosstalk_flow::store::{
    BusSink as FlowSink, PgChannelRegistry, PgTransmissionStore, UlidChannelIds,
};
use crosstalk_memory::analysis::catalog::RetentionPolicy;
use crosstalk_memory::analysis::fakes::FakeEmbedder;
use crosstalk_memory::surface::sinks::SinkConfig;
use crosstalk_provenance::store::PgProvenanceStore;
use crosstalk_reconstruct::agents::PgAgents;
use crosstalk_reconstruct::ids::{CONVERSATIONS_CURSOR_LABEL, UlidSource};
use crosstalk_reconstruct::publish::BusSink as ReconstructSink;
use crosstalk_reconstruct::thread::{PgConversations, ThreadConfig};
use crosstalk_spec::aggregates::alert::AlertRuleConfig;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::ids::{AgentId, ChannelId, KeyedHasher, SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BlobStore, EventBus};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::support::{Clock, Similarity};
use crosstalk_store::SerializableRetry;
use crosstalk_store::sqlx::PgPool;
use crosstalk_surface::export::{SpecExportSource, StoredTransmissions};
use crosstalk_surface::nodes::{NodeCache, NodeFeeder};
use crosstalk_surface::pg::{PgAuditLog, PgOperatorStore, PgSinkRegistry};
use crosstalk_surface::{SurfaceStores, pg as surface_pg};
use crosstalk_topology::env::Env;
use crosstalk_topology::outbox::OutboxRelay;
use crosstalk_topology::store::{EdgeStoreConfig, PgEdgeStore};
use crosstalk_transport::PgDeadLetters;

pub use evidence::PgEvidence;

use crate::in_process::HostedStores;

/// Cursor-key labels of the stores whose crates take a key rather than
/// the secret (the others derive their own: agents, conversations, audit,
/// the surface).
pub const EXCHANGES_CURSOR_LABEL: &str = "crosstalk.cursor.v1.exchanges";
pub const TOPICS_CURSOR_LABEL: &str = "crosstalk.cursor.v1.topics";
pub const SEARCH_CURSOR_LABEL: &str = "crosstalk.cursor.v1.search";
pub const PROJECTIONS_CURSOR_LABEL: &str = "crosstalk.cursor.v1.projections";
pub const ALERTS_CURSOR_LABEL: &str = "crosstalk.cursor.v1.alerts";

/// Where the random part of the ids the Postgres stores mint comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgIds {
    /// OS entropy for every generator: ids minted after a restart never
    /// repeat persisted ones (`surface.ids.unique-across-restart`). What a
    /// deployment uses.
    Entropy,
    /// Each generator seeded with `seed ^ purpose`, as the memory set
    /// seeds its own (`Live`'s L3 ids from `seed ^ salt`): a run is
    /// reproducible, and mints the ids a memory run with the same seed
    /// mints. For tests over a fresh database only.
    Seeded(u64),
}

impl PgIds {
    /// The random source of the generator for `purpose`.
    pub fn random(self, purpose: u64) -> SeededRandom {
        match self {
            Self::Entropy => SeededRandom::from_entropy(),
            Self::Seeded(seed) => SeededRandom::new(seed ^ purpose),
        }
    }
}

/// Purposes the generators are seeded for (any distinct constants).
mod purpose {
    pub const RECONSTRUCT_SINK: u64 = 0x5EC0_0001;
    pub const MERGES: u64 = 0x5EC0_0002;
    pub const FLOW_SINK: u64 = 0x5EC0_0003;
    pub const CHANNELS: u64 = 0x5EC0_0004;
    pub const ANALYSIS_SINK: u64 = 0x5EC0_0005;
    pub const ALERTS: u64 = 0x5EC0_0006;
    pub const OPERATORS: u64 = 0x5EC0_0007;
}

/// The reconstruct layer's outbox sink.
pub type PgReconstructSink<E> = ReconstructSink<E, SeededRandom>;
/// The flow layer's outbox sink.
pub type PgFlowSink<E> = FlowSink<E, SeededRandom>;
/// The analysis layer's outbox sink.
pub type PgAnalysisSink<E> = AnalysisSink<E, SeededRandom>;

/// L3's agent store as the bundle builds it.
pub type PgAgentStore<E> = PgAgents<PgReconstructSink<E>, UlidSource<SeededRandom>>;
/// L5's registry.
pub type PgChannels<E> = PgChannelRegistry<PgAgentStore<E>, PgFlowSink<E>>;
/// L5's transmissions.
pub type PgTransmissions<E> = PgTransmissionStore<PgAgentStore<E>, PgFlowSink<E>>;
/// L6's topic catalog.
pub type PgCatalog<E> = PgTopicCatalog<PgAgentStore<E>, PgAnalysisSink<E>>;
/// L6's search index.
pub type PgSearch<E> = PgSearchIndex<PgDirectory<E>, PgCatalog<E>>;
/// L6's projection store.
pub type PgProjections<E> = PgProjectionStore<PgAnalysisSink<E>>;
/// What alert rules read about their subjects.
pub type PgAlertFacts<E> = FlowFacts<PgTransmissions<E>, PgChannels<E>, PgDirectory<E>>;
/// L6's alert store.
pub type PgAlerts<E> =
    PgAlertStore<FakeEmbedder, PgDirectory<E>, PgAlertFacts<E>, PgAnalysisSink<E>>;
/// L7's edge store, reading the catalog, the directories and the node facts.
pub type PgEdges<E> = PgEdgeStore<Env<PgCatalog<E>, PgDirectory<E>, NodeCache>>;
/// What exports read.
pub type PgExport<E> = SpecExportSource<
    PgEdges<E>,
    PgProjections<E>,
    PgCatalog<E>,
    FakeEmbedder,
    StoredTransmissions<PgTransmissions<E>, PgDirectory<E>, PgCatalog<E>>,
>;

/// L3's merges and L5's supersessions, as the stores that resolve ids
/// read them (both from their in-process caches).
pub struct PgDirectory<E> {
    pub agents: PgAgentStore<E>,
    pub channels: PgChannels<E>,
}

impl<E> Clone for PgDirectory<E> {
    fn clone(&self) -> Self {
        Self {
            agents: self.agents.clone(),
            channels: self.channels.clone(),
        }
    }
}

impl<E> AgentDirectory for PgDirectory<E> {
    fn canonical(&self, id: AgentId) -> AgentId {
        AgentDirectory::canonical(&self.agents, id)
    }
}

impl<E> ChannelDirectory for PgDirectory<E> {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(&self.channels, id)
    }
}

/// The store settings the bundle is built with: the surface's options
/// that configure stores rather than the surface itself.
#[derive(Debug, Clone)]
pub struct PgSettings {
    pub bucket_width: BucketWidth,
    pub timing: CorrelationTiming,
    pub retention: RetentionPolicy,
    pub lineage_floor: Similarity,
    pub embedding_model: EmbeddingModel,
    pub default_remap_threshold: Similarity,
    pub projection_lease: Duration,
    pub frame_retention: Duration,
    /// The configured alert sinks.
    pub sinks: Vec<SinkConfig>,
    /// Whether this process stores `sinks` as the registry's configuration
    /// (the pipeline process), or only reads what is stored (the API role).
    pub configure_sinks: bool,
    pub threading: ThreadConfig,
    /// Every write transaction's retry policy.
    pub retry: SerializableRetry,
}

/// What [`PgStores::open`] builds from.
pub struct PgOpen<E, B> {
    /// The pool, every layer's migrations applied.
    pub pool: PgPool,
    /// Where the stores' outboxes relay to and `SetPolicy` publishes.
    pub bus: E,
    /// The bus's dead letters.
    pub dead_letters: PgDeadLetters,
    pub blobs: B,
    /// The injected clock: the sinks' stamps, the catalog's first version.
    pub clock: Arc<dyn Clock>,
    /// The deployment secret the cursor keys derive from.
    pub secret: Arc<KeyedHasher>,
    pub ids: PgIds,
    pub settings: PgSettings,
}

/// Why the bundle did not open. Names the store, never a row's content.
#[derive(Debug, thiserror::Error)]
pub enum PgStoresError {
    #[error("opening {store}: {reason}")]
    Open { store: &'static str, reason: String },
}

fn open_failed(store: &'static str) -> impl FnOnce(&dyn std::fmt::Debug) -> PgStoresError {
    move |error| PgStoresError::Open {
        store,
        reason: format!("{error:?}"),
    }
}

/// Every store the surface reads, on Postgres. Clones share every store.
pub struct PgStores<E, B> {
    pub pool: PgPool,
    pub agents: PgAgentStore<E>,
    pub channels: PgChannels<E>,
    pub transmissions: PgTransmissions<E>,
    pub catalog: PgCatalog<E>,
    pub search: PgSearch<E>,
    pub embedder: FakeEmbedder,
    pub projections: PgProjections<E>,
    pub alerts: PgAlerts<E>,
    pub edges: PgEdges<E>,
    pub audit: PgAuditLog,
    pub operators: PgOperatorStore,
    pub sinks: PgSinkRegistry,
    pub dead_letters: PgDeadLetters,
    pub bus: E,
    pub blobs: B,
    pub evidence: PgEvidence,
    pub export: PgExport<E>,
    pub nodes: NodeCache,
    pub exchanges: PgExchanges,
    pub conversations: PgConversations,
    pub provenance: PgProvenanceStore,
}

impl<E: Clone, B: Clone> Clone for PgStores<E, B> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            agents: self.agents.clone(),
            channels: self.channels.clone(),
            transmissions: self.transmissions.clone(),
            catalog: self.catalog.clone(),
            search: self.search.clone(),
            embedder: self.embedder.clone(),
            projections: self.projections.clone(),
            alerts: self.alerts.clone(),
            edges: self.edges.clone(),
            audit: self.audit.clone(),
            operators: self.operators.clone(),
            sinks: self.sinks.clone(),
            dead_letters: self.dead_letters.clone(),
            bus: self.bus.clone(),
            blobs: self.blobs.clone(),
            evidence: self.evidence.clone(),
            export: self.export.clone(),
            nodes: self.nodes.clone(),
            exchanges: self.exchanges.clone(),
            conversations: self.conversations.clone(),
            provenance: self.provenance.clone(),
        }
    }
}

fn cursor_key(secret: &KeyedHasher, label: &'static str) -> [u8; 32] {
    *secret.derive_key(label).as_bytes()
}

impl<E, B> PgStores<E, B>
where
    E: EventBus + Clone + Send + Sync + 'static,
    B: BlobStore + Clone + Send + Sync + 'static,
{
    /// Open every store over `open.pool`. The stores that load caches load
    /// them; L6's stores publish what an earlier run left in their outboxes
    /// first. Returns the edge store's outbox relay, which the caller runs
    /// (`OutboxRelay::run`): until it runs, L7's committed events wait in
    /// `topology.outbox`.
    pub async fn open(open: PgOpen<E, B>) -> Result<(Self, OutboxRelay), PgStoresError> {
        let PgOpen {
            pool,
            bus,
            dead_letters,
            blobs,
            clock,
            secret,
            ids,
            settings,
        } = open;
        let generator = |purpose: u64| UlidGenerator::new(Arc::clone(&clock), ids.random(purpose));
        let shared_bus = Arc::new(bus.clone());
        let reconstruct_sink = ReconstructSink::new(
            Arc::clone(&shared_bus),
            Arc::clone(&clock),
            generator(purpose::RECONSTRUCT_SINK),
        );
        let agents: PgAgentStore<E> = PgAgents::open_with_secret(
            pool.clone(),
            reconstruct_sink,
            UlidSource::new(generator(purpose::MERGES)),
            &secret,
        )
        .await
        .map_err(|error| open_failed("agents")(&error))?
        .with_retry(settings.retry);
        let flow_sink = || {
            FlowSink::new(
                bus.clone(),
                Arc::clone(&clock),
                generator(purpose::FLOW_SINK),
            )
        };
        let channels: PgChannels<E> = PgChannelRegistry::open(
            pool.clone(),
            agents.clone(),
            Arc::new(UlidChannelIds::new(generator(purpose::CHANNELS))),
            flow_sink(),
        )
        .await
        .map_err(|error| open_failed("channels")(&error))?
        .with_retry(settings.retry);
        let transmissions: PgTransmissions<E> =
            PgTransmissionStore::new(pool.clone(), agents.clone(), flow_sink())
                .with_retry(settings.retry);
        let directory = PgDirectory {
            agents: agents.clone(),
            channels: channels.clone(),
        };
        let analysis_sink = Arc::new(AnalysisSink::new(
            Arc::clone(&shared_bus),
            Arc::clone(&clock),
            generator(purpose::ANALYSIS_SINK),
        ));
        let catalog: PgCatalog<E> = PgTopicCatalog::open(
            pool.clone(),
            CatalogConfig {
                retention: settings.retention,
                lineage_floor: settings.lineage_floor,
            },
            clock.now(),
            CatalogParts {
                agents: agents.clone(),
                sink: Arc::clone(&analysis_sink),
                cursor_key: CursorKey::new(cursor_key(&secret, TOPICS_CURSOR_LABEL)),
                retry: settings.retry,
            },
        )
        .await
        .map_err(|error| open_failed("topic catalog")(&error))?;
        let search: PgSearch<E> = PgSearchIndex::open(
            pool.clone(),
            directory.clone(),
            catalog.clone(),
            CursorKey::new(cursor_key(&secret, SEARCH_CURSOR_LABEL)),
            settings.embedding_model.clone(),
        )
        .await
        .map_err(|error| open_failed("search index")(&error))?;
        let embedder = FakeEmbedder::new(settings.embedding_model.clone(), 8_000);
        let projections: PgProjections<E> = PgProjectionStore::open(
            pool.clone(),
            ProjectionStoreConfig {
                lease: settings.projection_lease,
                frame_retention: settings.frame_retention,
            },
            ProjectionParts {
                sink: Arc::clone(&analysis_sink),
                cursor_key: CursorKey::new(cursor_key(&secret, PROJECTIONS_CURSOR_LABEL)),
                retry: settings.retry,
            },
        )
        .await
        .map_err(|error| open_failed("projections")(&error))?;
        let alerts: PgAlerts<E> = PgAlertStore::open(
            pool.clone(),
            AlertStoreConfig {
                rules: AlertRuleConfig {
                    default_remap_threshold: settings.default_remap_threshold,
                },
                sinks: settings
                    .sinks
                    .iter()
                    .map(|sink| sink.id)
                    .collect::<BTreeSet<_>>(),
                builtins: Default::default(),
            },
            AlertStoreParts {
                embedder: embedder.clone(),
                directory: directory.clone(),
                facts: FlowFacts {
                    transmissions: transmissions.clone(),
                    channels: channels.clone(),
                    directory: directory.clone(),
                },
                sink: Arc::clone(&analysis_sink),
                ids: generator(purpose::ALERTS),
                cursor_key: CursorKey::new(cursor_key(&secret, ALERTS_CURSOR_LABEL)),
                retry: settings.retry,
            },
        )
        .await
        .map_err(|error| open_failed("alerts")(&error))?;
        let nodes = NodeCache::new();
        let (edges, relay) = PgEdgeStore::new(
            pool.clone(),
            EdgeStoreConfig {
                bucket_width: settings.bucket_width,
                timing: settings.timing,
                partition_span: Default::default(),
            },
            Env {
                catalog: catalog.clone(),
                directory: directory.clone(),
                nodes: nodes.clone(),
            },
        );
        let audit = PgAuditLog::new(pool.clone(), settings.retry, &secret);
        let operators =
            PgOperatorStore::new(pool.clone(), settings.retry, ids.random(purpose::OPERATORS));
        let sinks = match settings.configure_sinks {
            true => PgSinkRegistry::configure(
                pool.clone(),
                settings.retry,
                settings.sinks.iter().map(|sink| surface_pg::SinkConfig {
                    id: sink.id,
                    kind: sink.kind,
                    name: sink.name.clone(),
                }),
            )
            .await
            .map_err(|error| open_failed("sinks")(&error))?,
            false => PgSinkRegistry::open(pool.clone(), settings.retry),
        };
        let export = SpecExportSource::new(
            edges.clone(),
            projections.clone(),
            catalog.clone(),
            embedder.clone(),
        )
        .with_transmissions(StoredTransmissions::new(
            transmissions.clone(),
            directory.clone(),
            catalog.clone(),
        ));
        let provenance = PgProvenanceStore::new(pool.clone());
        let evidence = PgEvidence::new(provenance.clone(), pool.clone());
        let stores = Self {
            agents,
            channels,
            transmissions,
            catalog,
            search,
            embedder,
            projections,
            alerts,
            edges,
            audit,
            operators,
            sinks,
            dead_letters,
            bus,
            blobs,
            evidence,
            export,
            nodes,
            exchanges: PgExchanges::new(pool.clone())
                .with_cursor_key(cursor_key(&secret, EXCHANGES_CURSOR_LABEL)),
            conversations: PgConversations::new(pool.clone())
                .with_config(settings.threading)
                .with_retry(settings.retry)
                .with_cursor_secret(&secret),
            provenance,
            pool,
        };
        // The conversation reads' key is derived inside the store; named
        // here so the label list in the docs stays complete.
        let _ = CONVERSATIONS_CURSOR_LABEL;
        tracing::info!("postgres stores opened");
        Ok((stores, relay))
    }

    /// The directory over the bundle's agent and channel stores.
    pub fn directory(&self) -> PgDirectory<E> {
        PgDirectory {
            agents: self.agents.clone(),
            channels: self.channels.clone(),
        }
    }
}

impl<E, B> SurfaceStores for PgStores<E, B>
where
    E: EventBus + Clone + Send + Sync + 'static,
    B: BlobStore + Clone + Send + Sync + 'static,
{
    type Agents = PgAgentStore<E>;
    type Channels = PgChannels<E>;
    type Transmissions = PgTransmissions<E>;
    type Topics = PgCatalog<E>;
    type Search = PgSearch<E>;
    type Embedder = FakeEmbedder;
    type Projections = PgProjections<E>;
    type Alerts = PgAlerts<E>;
    type Edges = PgEdges<E>;
    type Audit = PgAuditLog;
    type Operators = PgOperatorStore;
    type Sinks = PgSinkRegistry;
    type DeadLetters = PgDeadLetters;
    type Bus = E;
    type Blobs = B;
    type Evidence = PgEvidence;
    type Export = PgExport<E>;
    type Exchanges = PgExchanges;
    type Conversations = PgConversations;
    type Provenance = PgProvenanceStore;

    fn agents(&self) -> &Self::Agents {
        &self.agents
    }
    fn channels(&self) -> &Self::Channels {
        &self.channels
    }
    fn transmissions(&self) -> &Self::Transmissions {
        &self.transmissions
    }
    fn topics(&self) -> &Self::Topics {
        &self.catalog
    }
    fn search(&self) -> &Self::Search {
        &self.search
    }
    fn embedder(&self) -> &FakeEmbedder {
        &self.embedder
    }
    fn projections(&self) -> &Self::Projections {
        &self.projections
    }
    fn alerts(&self) -> &Self::Alerts {
        &self.alerts
    }
    fn edges(&self) -> &Self::Edges {
        &self.edges
    }
    fn audit(&self) -> &PgAuditLog {
        &self.audit
    }
    fn operators(&self) -> &PgOperatorStore {
        &self.operators
    }
    fn sinks(&self) -> &PgSinkRegistry {
        &self.sinks
    }
    fn dead_letters(&self) -> &PgDeadLetters {
        &self.dead_letters
    }
    fn bus(&self) -> &E {
        &self.bus
    }
    fn blobs(&self) -> &B {
        &self.blobs
    }
    fn evidence(&self) -> &Self::Evidence {
        &self.evidence
    }
    fn export_source(&self) -> &Self::Export {
        &self.export
    }
    fn exchanges(&self) -> &PgExchanges {
        &self.exchanges
    }
    fn conversations(&self) -> &PgConversations {
        &self.conversations
    }
    fn provenance(&self) -> &PgProvenanceStore {
        &self.provenance
    }
}

impl<E, B> HostedStores for PgStores<E, B>
where
    E: EventBus + Clone + Send + Sync + 'static,
    B: BlobStore + Clone + Send + Sync + 'static,
{
    type NodeAgents = PgAgentStore<E>;
    type NodeChannels = PgChannels<E>;

    fn node_cache(&self) -> &NodeCache {
        &self.nodes
    }

    fn node_feeder(&self) -> NodeFeeder<PgAgentStore<E>, PgChannels<E>> {
        NodeFeeder::new(
            self.nodes.clone(),
            self.agents.clone(),
            self.channels.clone(),
        )
    }

    fn operator_handle(&self) -> PgOperatorStore {
        self.operators.clone()
    }
}
