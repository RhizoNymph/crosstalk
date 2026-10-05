//! The surface over the in-memory reference stores, in this process: the
//! backend a UI links in tests and development before the HTTP server
//! (P7.1) exists.
//!
//! ```text
//! InProcess::start(options)
//!   reference stores ── one Outbox ──▶ relay task ─┬─▶ NodeFeeder::apply  (graph node facts)
//!        ▲                                         └─▶ FeedHandle::append (Changed → live feed)
//!        │ seed through the spec's write traits (`InProcess::stores`)
//!   Surface<MemoryStores> ◀── QueryApi / OperatorActions / LiveFeed (`InProcess::surface`)
//!   operators: OperatorStore::load(options.access) at start; callers from `InProcess::caller`
//! ```
//!
//! Thin by design: every rule lives in `crosstalk-surface` and the stores;
//! this module only builds them and relays what the stores publish to the
//! two consumers the surface needs (in a gateway, bus consumer groups do).
//!
//! [`InProcess::start`] builds its own [`Backbone`]: an in-memory blob
//! store, a bus, and an outbox whose receiver is the relay's input. A
//! composer that runs the layers too (`crosstalk_gateway::live::Live`)
//! calls [`InProcess::start_with`] with its own: its bus and blob store,
//! an outbox it forwards onto that bus, and a relay input fed from a bus
//! subscription, so the node facts and the live feed see every event on
//! the bus, not only the stores' own.

mod stores;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::analysis::alerts::{AlertStoreConfig, InMemoryAlertStore};
use crosstalk_memory::analysis::catalog::{CatalogConfig, InMemoryTopicCatalog, RetentionPolicy};
use crosstalk_memory::analysis::fakes::FakeEmbedder;
use crosstalk_memory::analysis::projection::{InMemoryProjectionStore, ProjectionConfig};
use crosstalk_memory::analysis::search::InMemorySearchIndex;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::{IdSequence, Outbox};
use crosstalk_memory::surface::audit::InMemoryAuditLog;
use crosstalk_memory::surface::operators::InMemoryOperatorStore;
use crosstalk_memory::surface::sinks::{InMemorySinkRegistry, SinkConfig};
use crosstalk_memory::topology::env::Env;
use crosstalk_memory::topology::store::{EdgeStoreConfig, InMemoryEdgeStore};
use crosstalk_spec::aggregates::alert::AlertRuleConfig;
use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::aggregates::topic::EmbeddingModel;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::{ConfigHash, SeededRandom};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycleError;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::live::FeedEpoch;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, CallerError, OperatorLoadError, OperatorStore, RequestIdentity,
};
use crosstalk_spec::support::{Blake3, Clock, Similarity};
use crosstalk_surface::export::{SpecExportSource, StoredTransmissions};
use crosstalk_surface::live::{FeedClosed, FeedHandle, FeedWriter};
use crosstalk_surface::nodes::{NodeCache, NodeFeedError, NodeFeeder};
use crosstalk_surface::{Surface, SurfaceConfig};
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus, StartError};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

pub use stores::{Alerts, Directory, Edges, Export, MemoryEvidence, MemoryStores, Search};

/// How the in-process surface and its stores are configured.
#[derive(Clone)]
pub struct InProcessOptions {
    /// Stamps every acceptance time and `present`; the stores take their
    /// times as arguments from the callers that seed them.
    pub clock: Arc<dyn Clock>,
    /// Seeds the ids the surface mints and its cursor key.
    pub seed: u64,
    pub surface: SurfaceConfig,
    /// Who may use the surface (trusted mode: one operator with every
    /// permission).
    pub access: AccessConfig,
    /// L7's bucket width.
    pub bucket_width: BucketWidth,
    /// The correlator's timing, which the watermark trails by.
    pub timing: CorrelationTiming,
    /// How many activated topic-model versions retention keeps.
    pub retention: RetentionPolicy,
    /// Every non-best topic lineage link is at or above it.
    pub lineage_floor: Similarity,
    /// The embedding model search and semantic rules embed with.
    pub embedding_model: EmbeddingModel,
    /// The configured alert sinks.
    pub sinks: Vec<SinkConfig>,
    /// How long a claimed projection job is held.
    pub projection_lease: Duration,
}

/// Why the in-process surface could not start.
#[derive(Debug, thiserror::Error)]
pub enum InProcessError {
    #[error("the topic catalog refused its start: {0:?}")]
    Catalog(TopicLifecycleError),
    #[error("the bus could not start: {0:?}")]
    Bus(StartError),
    #[error("the access config could not be loaded: {0:?}")]
    Operators(OperatorLoadError),
    #[error("the node facts could not be rebuilt: {0}")]
    Nodes(NodeFeedError),
    #[error("the live feed stopped: {0}")]
    Feed(FeedClosed),
}

/// What the stores run over, supplied by whoever hosts them.
pub struct Backbone<B> {
    /// The bus the surface reads depths and dead letters from.
    pub bus: MpscBus,
    /// Where bodies are stored, and evidence excerpts cut from.
    pub blobs: B,
    /// Every store publishes into it.
    pub outbox: Outbox,
    /// What the relay hands to the node facts and the live feed, in order:
    /// the outbox's own receiver when nothing else consumes the stores'
    /// events, or a bus subscription's events when the outbox is
    /// forwarded onto the bus.
    pub events: UnboundedReceiver<BusEvent>,
}

impl Backbone<MemoryBlobStore> {
    /// A backbone of its own: a fresh bus and in-memory blob store, and the
    /// outbox relayed straight to the surface.
    pub fn standalone() -> Result<Self, InProcessError> {
        let (outbox, events) = Outbox::channel();
        Ok(Self {
            bus: MpscBus::start(BusConfig::default()).map_err(InProcessError::Bus)?,
            blobs: MemoryBlobStore::new(),
            outbox,
            events,
        })
    }
}

/// The surface over the reference stores, and handles on both.
pub struct InProcess<B = MemoryBlobStore> {
    /// The stores: seed the world through the spec's write traits on them.
    pub stores: MemoryStores<B>,
    /// The surface: `QueryApi`, `OperatorActions` and `LiveFeed`.
    pub surface: Arc<Surface<MemoryStores<B>>>,
    /// Keeps the graphs' node facts current; the relay feeds it.
    pub nodes: NodeFeeder<MemoryAgents, MemoryChannels<MemoryAgents>>,
    relay: JoinHandle<()>,
}

impl InProcess<MemoryBlobStore> {
    /// [`InProcess::start_with`] over a [`Backbone::standalone`].
    pub async fn start(options: InProcessOptions) -> Result<Self, InProcessError> {
        Self::start_with(options, Backbone::standalone()?).await
    }
}

impl<B> InProcess<B>
where
    B: BlobStore + Clone + Send + Sync + 'static,
{
    /// Build every store over `backbone`, load `options.access`, rebuild
    /// the node facts, start the live feed and the relay, and build the
    /// surface. Needs a tokio runtime.
    pub async fn start_with(
        options: InProcessOptions,
        backbone: Backbone<B>,
    ) -> Result<Self, InProcessError> {
        let Backbone {
            bus,
            blobs,
            outbox,
            events: published,
        } = backbone;
        let started = options.clock.now();
        let agents = MemoryAgents::new(IdSequence::new(1 << 90), outbox.clone());
        let channels =
            MemoryChannels::new(agents.clone(), IdSequence::new(2 << 90), outbox.clone());
        let directory = Directory {
            agents: agents.clone(),
            channels: channels.clone(),
        };
        let transmissions =
            MemoryVerdicts::with_directories(directory.clone(), directory.clone(), outbox.clone());
        let catalog = InMemoryTopicCatalog::new(
            CatalogConfig {
                retention: options.retention,
                lineage_floor: options.lineage_floor,
            },
            started,
            outbox.clone(),
        )
        .map_err(InProcessError::Catalog)?;
        let embedder = FakeEmbedder::new(options.embedding_model.clone(), 8_000);
        let search = InMemorySearchIndex::new(
            catalog.clone(),
            directory.clone(),
            options.embedding_model.clone(),
        );
        let projections = InMemoryProjectionStore::new(
            ProjectionConfig {
                lease: options.projection_lease,
                frame_retention: options.surface.frame_retention.as_duration(),
            },
            outbox.clone(),
        );
        let alerts = InMemoryAlertStore::new(
            AlertStoreConfig {
                rules: AlertRuleConfig {
                    default_remap_threshold: options.surface.default_remap_threshold,
                },
                sinks: options
                    .sinks
                    .iter()
                    .map(|sink| sink.id)
                    .collect::<BTreeSet<_>>(),
                builtins: Default::default(),
            },
            embedder.clone(),
            directory.clone(),
            transmissions.clone(),
            outbox.clone(),
        );
        let nodes = NodeCache::new();
        let edges = InMemoryEdgeStore::new(
            EdgeStoreConfig {
                bucket_width: options.bucket_width,
                timing: options.timing,
            },
            Env {
                topics: catalog.clone(),
                directory: directory.clone(),
                nodes: nodes.clone(),
            },
            outbox.clone(),
        );
        let audit = InMemoryAuditLog::new();
        let operators = InMemoryOperatorStore::new(audit.clone(), IdSequence::new(3 << 90));
        let export = SpecExportSource::new(
            edges.clone(),
            projections.clone(),
            catalog.clone(),
            embedder.clone(),
        )
        .with_transmissions(StoredTransmissions::new(
            transmissions.clone(),
            directory.clone(),
        ));
        let stores = MemoryStores {
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
            sinks: InMemorySinkRegistry::new(options.sinks.clone()),
            dead_letters: bus.dead_letters(),
            bus,
            blobs,
            evidence: MemoryEvidence::default(),
            export,
            nodes,
        };
        let feed = FeedWriter::spawn(options.surface.live, FeedEpoch(started.as_micros()));
        let feeder = NodeFeeder::new(
            stores.nodes.clone(),
            stores.agents.clone(),
            stores.channels.clone(),
        );
        let hash = ConfigHash::from_digest(Blake3::of(b"in-process access config"));
        let mut operator_store = stores.operators.clone();
        let changes = operator_store
            .load(&options.access, hash, started)
            .await
            .map_err(InProcessError::Operators)?;
        feed.config_loaded(&changes)
            .await
            .map_err(InProcessError::Feed)?;
        feeder.rebuild().await.map_err(InProcessError::Nodes)?;
        let relay = tokio::spawn(relay(published, feeder.clone(), feed.clone()));
        let surface = Arc::new(Surface::new(
            stores.clone(),
            options.clock,
            options.surface,
            SeededRandom::new(options.seed),
            feed,
        ));
        tracing::info!(operators = changes.len(), "in-process surface started");
        Ok(Self {
            stores,
            surface,
            nodes: feeder,
            relay,
        })
    }

    /// The caller of one request, from the loaded access config.
    pub async fn caller(&self, identity: RequestIdentity) -> Result<Caller, CallerError> {
        self.stores.operators.caller(identity).await
    }

    /// Stop the relay and the live feed; open streams end with
    /// `ShuttingDown`.
    pub async fn shutdown(self) {
        self.relay.abort();
        self.surface.feed().shutdown().await;
    }
}

/// Hand every event the stores publish to the node facts and, for a
/// `Changed`, to the live feed, in publish order.
async fn relay(
    mut published: UnboundedReceiver<BusEvent>,
    nodes: NodeFeeder<MemoryAgents, MemoryChannels<MemoryAgents>>,
    feed: FeedHandle,
) {
    while let Some(event) = published.recv().await {
        if let Err(error) = nodes.apply(&event).await {
            tracing::warn!(error = %error, "node facts not refreshed");
        }
        if let BusEvent::Changed(changed) = event
            && let Err(error) = feed.append(changed).await
        {
            tracing::warn!(error = %error, "live feed stopped; relay ends");
            return;
        }
    }
}
