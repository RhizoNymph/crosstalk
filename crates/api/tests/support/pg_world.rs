//! A seeded synthetic world on the Postgres stores, for the L8
//! conformance suite's Postgres harnesses: in process
//! (`crates/api/tests/conformance_pg.rs`) and over HTTP
//! (`crates/client/tests/conformance_pg.rs`, which includes this file by
//! path).
//!
//! ```text
//! Databases::new        probe: TestDb::new_or_skip (on a helper multi-threaded runtime,
//!                       since TestDb refuses the suite's current-thread one); none
//!                       configured: every test skips (Harness::unavailable)
//! provision(scenario)
//!   a fresh database, every layer's migrations and the extensions
//!   PgStores::open over a pool of the test's runtime; bus = Forwarding (every
//!     published event to the surface's relay channel, as the memory outbox)
//!   InProcess::host (operators loaded, node facts, CursorSecret::Derived)
//!   World::seed(&mut PgSeeding(stores))   every seeded write through Postgres, in the
//!                                         memory harness's order (surface hosted first)
//!   L7's outbox relay runs; wait until topology.outbox is empty
//!   settle (the relay applied every seeded event), clock to the anchor
//!   projection fitter over PgProjectionSource (FakeLayoutFitter, as in memory)
//!   bind the scenario from the stores' reads
//! ```
//!
//! Every step is bounded by [`WAIT`]. Each world has its own database,
//! dropped with it (or with the half-built world when provisioning fails).

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use crosstalk_analysis::search::PgProjectionSource;
use crosstalk_api::pg::{
    PgAgentStore, PgAlerts, PgCatalog, PgChannels, PgEdges, PgProjections, PgSearch,
    PgTransmissions,
};
use crosstalk_api::{
    CursorSecret, InProcess, InProcessOptions, PgEvidence, PgIds, PgOpen, PgSettings, PgStores,
    ProjectionFitting,
};
use crosstalk_conformance::harness::{Provision, ProvisionError};
use crosstalk_conformance::scenario::Bindings;
use crosstalk_conformance::world::{WorldReads, bind};
use crosstalk_memory::analysis::fakes::FakeLayoutFitter;
use crosstalk_memory::analysis::search::FixedWatermark;
use crosstalk_memory::surface::sinks::SinkConfig;
use crosstalk_provenance::store::PgProvenanceStore;
use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    FitFailure, PointParts, ProjectedPoint, ProjectionInfo,
};
use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::batch::IdBatch;
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{
    AccessId, ChannelId, DeploymentSecret, KeyedHasher, SecretVersion, SeededRandom,
};
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, EventBus, RetryPolicy};
use crosstalk_spec::interfaces::l5_flow::channels::{
    AccessReadError, AccessStore, ChannelReads, ChannelWithTraffic,
};
use crosstalk_spec::interfaces::l5_flow::{ChannelDirectory, RegistryError};
use crosstalk_spec::interfaces::l6_analysis::{
    LayoutError, LayoutFitter, ProjectionSource, ProjectionStore, SampleError,
};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::export::{ExportFormat, ExportFormats, GatewayVersion};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::operators::{AccessConfig, OperatorConfig};
use crosstalk_spec::interfaces::l8_surface::{PermissionSet, QueryApi};
use crosstalk_spec::paging::{ChannelList, ChannelTransmissionList, Page, PageRequest};
use crosstalk_spec::support::{Clock, Finite, TimeWindow, Timestamp};
use crosstalk_store::sqlx::postgres::PgPoolOptions;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_store::{Layer, Migrations, SerializableRetry, TestDb};
use crosstalk_surface::pg::{PgAuditLog, PgOperatorStore, PgSinkRegistry};
use crosstalk_surface::{EvidenceRecords, SurfaceConfig};
use crosstalk_topology::outbox::{BusAnnouncer, OutboxIds};
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus, PgBus, PgBusConfig, PgDeadLetters};
use crosstalk_world::clock::{BUCKET, DAY, MINUTE, UI_ANCHOR, minus, plus};
use crosstalk_world::config::OPERATOR_RESEARCHER;
use crosstalk_world::{Anchor, World, WorldClock, WorldStores};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

/// The seed every world is generated from (the memory harness's).
pub const SEED: u64 = 7;

/// How long any one step of a provisioning (creating the database,
/// migrating, seeding, settling) may take before it fails with its name.
pub const WAIT: Duration = Duration::from_secs(300);

/// The deployment secret the cursor keys derive from.
pub const SECRET_HEX: &str = "c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0";

/// How many connections each world's pool holds.
pub const POOL: u32 = 8;

/// The bus the stores publish to: every event goes to the surface's relay
/// channel, as the memory stores' outbox does (the harness runs no
/// pipeline, so nothing else consumes them). Subscriptions are an idle
/// in-memory bus's.
#[derive(Clone)]
pub struct Forwarding {
    events: UnboundedSender<BusEvent>,
    idle: MpscBus,
}

impl EventBus for Forwarding {
    type Subscription = <MpscBus as EventBus>::Subscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        self.events
            .send(envelope.event)
            .map_err(|_| BusError::Disconnected)
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<Self::Subscription, BusError> {
        self.idle.subscribe(subjects, group, retry).await
    }
}

pub type Stores = PgStores<Forwarding, MemoryBlobStore>;

/// The Postgres bundle as the world's [`WorldStores`]: the seed writes
/// through the spec's write traits on handles sharing the surface's pool.
pub struct PgSeeding(pub Stores);

impl WorldStores for PgSeeding {
    type Agents = PgAgentStore<Forwarding>;
    type Spans = PgProvenanceStore;
    type Channels = PgChannels<Forwarding>;
    type Transmissions = PgTransmissions<Forwarding>;
    type Catalog = PgCatalog<Forwarding>;
    type Search = PgSearch<Forwarding>;
    type Edges = PgEdges<Forwarding>;
    type Alerts = PgAlerts<Forwarding>;
    type Projections = PgProjections<Forwarding>;
    type Operators = PgOperatorStore;
    type Audit = PgAuditLog;
    type Sinks = PgSinkRegistry;
    type Letters = PgDeadLetters;
    type Blobs = MemoryBlobStore;

    fn agents(&mut self) -> &mut Self::Agents {
        &mut self.0.agents
    }
    fn spans(&mut self) -> &mut Self::Spans {
        &mut self.0.provenance
    }
    fn channels(&mut self) -> &mut Self::Channels {
        &mut self.0.channels
    }
    fn transmissions(&mut self) -> &mut Self::Transmissions {
        &mut self.0.transmissions
    }
    fn catalog(&mut self) -> &mut Self::Catalog {
        &mut self.0.catalog
    }
    fn search(&mut self) -> &mut Self::Search {
        &mut self.0.search
    }
    fn edges(&mut self) -> &mut Self::Edges {
        &mut self.0.edges
    }
    fn alerts(&mut self) -> &mut Self::Alerts {
        &mut self.0.alerts
    }
    fn projections(&mut self) -> &mut Self::Projections {
        &mut self.0.projections
    }
    fn operators(&mut self) -> &mut Self::Operators {
        &mut self.0.operators
    }
    fn audit(&mut self) -> &mut Self::Audit {
        &mut self.0.audit
    }
    fn sinks(&mut self) -> &mut Self::Sinks {
        &mut self.0.sinks
    }
    fn letters(&mut self) -> &mut Self::Letters {
        &mut self.0.dead_letters
    }
    fn blobs(&mut self) -> &mut Self::Blobs {
        &mut self.0.blobs
    }
}

/// The registry's reads for [`bind`], with the accesses it needs read as
/// the surface's evidence page reads them (`PgEvidence`): the Postgres
/// registry does not implement the spec's `AccessStore` yet
/// (`docs/features/postgres_mode.md`, Gaps).
pub struct BindChannels<'a> {
    pub channels: &'a PgChannels<Forwarding>,
    pub evidence: &'a PgEvidence,
}

impl ChannelDirectory for BindChannels<'_> {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(self.channels, id)
    }
}

impl ChannelReads for BindChannels<'_> {
    async fn channel(&self, id: ChannelId) -> Result<Option<ChannelWithTraffic>, RegistryError> {
        self.channels.channel(id).await
    }

    async fn channels(
        &self,
        filter: &ChannelFilter,
        page: &PageRequest<ChannelList>,
    ) -> Result<Page<ChannelWithTraffic, ChannelList>, RegistryError> {
        self.channels.channels(filter, page).await
    }

    async fn transmissions(
        &self,
        channel: ChannelId,
        filter: &ChannelTransmissionFilter,
        page: &PageRequest<ChannelTransmissionList>,
    ) -> Result<
        Page<crosstalk_spec::derived::flow::transmission::Transmission, ChannelTransmissionList>,
        RegistryError,
    > {
        self.channels.transmissions(channel, filter, page).await
    }
}

impl AccessStore for BindChannels<'_> {
    async fn accesses(
        &self,
        ids: &IdBatch<AccessId>,
    ) -> Result<std::collections::BTreeMap<AccessId, (Access, Resource)>, AccessReadError> {
        let failed = |error: crosstalk_surface::RecordReadError| AccessReadError::Store {
            reason: error.to_string(),
        };
        let mut found = std::collections::BTreeMap::new();
        for id in ids.ids() {
            let Some(access) = self.evidence.access(*id).await.map_err(failed)? else {
                continue;
            };
            let Some(resource) = self
                .evidence
                .resource(access.resource)
                .await
                .map_err(failed)?
            else {
                continue;
            };
            found.insert(*id, (access, resource));
        }
        Ok(found)
    }
}

/// The present the stores and the surface read: a minute before the
/// world's first config load while it is seeded (every seeded write
/// carries its own time), the anchor once served (as `seed_world`'s).
#[derive(Debug)]
pub struct SeedClock {
    anchor: Anchor,
    serving: OnceLock<WorldClock>,
}

impl Clock for SeedClock {
    fn now(&self) -> Timestamp {
        match self.serving.get() {
            Some(clock) => clock.now(),
            None => minus(self.anchor.config_at(), MINUTE),
        }
    }
}

/// A helper multi-threaded runtime: `TestDb` creates and drops databases
/// only on one, and the suite drives each test on a current-thread one.
pub fn pg_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap_or_else(|error| panic!("the helper runtime: {error}"))
    })
}

/// `work` run to completion on [`pg_runtime`], from any thread (a thread of
/// its own blocks on it, so a caller inside another runtime may wait).
pub fn on_pg<F>(work: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| pg_runtime().block_on(work))
            .join()
            .unwrap_or_else(|_| panic!("the helper runtime's work panicked"))
    })
}

/// A fresh migrated database, `None` without a test server.
pub fn migrated_database(test: &'static str) -> Result<Option<TestDb>, String> {
    on_pg(async move {
        let created = tokio::time::timeout(WAIT, TestDb::new_or_skip(test))
            .await
            .map_err(|_| format!("creating the test database took over {WAIT:?}"))?
            .map_err(|error| format!("the test database: {error}"))?;
        let Some(db) = created else {
            return Ok(None);
        };
        let migrate = async {
            db.ensure_extensions().await?;
            for (layer, migrator) in [
                (Layer::Transport, &crosstalk_transport::pg::MIGRATIONS),
                (
                    Layer::Canonical,
                    &crosstalk_canonical::exchanges::MIGRATIONS,
                ),
                (
                    Layer::Reconstruct,
                    &crosstalk_reconstruct::agents::MIGRATIONS,
                ),
                (Layer::Provenance, &crosstalk_provenance::store::MIGRATIONS),
                (Layer::Flow, &crosstalk_flow::store::MIGRATIONS),
                (Layer::Analysis, &crosstalk_analysis::pg::MIGRATIONS),
                (Layer::Topology, &crosstalk_topology::store::MIGRATIONS),
                (Layer::Surface, &crosstalk_surface::pg::MIGRATIONS),
            ] {
                db.migrate(layer, Migrations::Embedded(migrator)).await?;
            }
            Ok::<(), crosstalk_store::StoreError>(())
        };
        tokio::time::timeout(WAIT, migrate)
            .await
            .map_err(|_| format!("migrating took over {WAIT:?}"))?
            .map_err(|error| format!("migrating: {error}"))?;
        Ok(Some(db))
    })
}

/// Where the worlds' databases come from: the availability probe's
/// database first, then a fresh one per world.
pub struct Databases {
    /// The database the availability probe created, for the first world.
    probe: Mutex<DbGuard>,
    /// Why the harness cannot run: no test server configured.
    unavailable: Option<String>,
    /// Why the probe failed although a server is configured.
    broken: Option<String>,
}

impl Databases {
    pub fn new() -> Self {
        match migrated_database("conformance_pg") {
            Ok(Some(db)) => Self {
                probe: Mutex::new(DbGuard(Some(db))),
                unavailable: None,
                broken: None,
            },
            Ok(None) => Self {
                probe: Mutex::new(DbGuard(None)),
                unavailable: Some("TEST_DATABASE_URL is not set".to_owned()),
                broken: None,
            },
            Err(reason) => Self {
                probe: Mutex::new(DbGuard(None)),
                unavailable: None,
                broken: Some(reason),
            },
        }
    }

    /// The probe's database if still unused, else a fresh one.
    pub fn database(&self) -> Result<TestDb, String> {
        if let Some(reason) = &self.broken {
            return Err(reason.clone());
        }
        let probe = self
            .probe
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .0
            .take();
        match probe {
            Some(db) => Ok(db),
            None => migrated_database("conformance_pg")?
                .ok_or_else(|| "the test server went away".to_owned()),
        }
    }

    /// Why no world can be provisioned here: no test server configured.
    pub fn unavailable(&self) -> Option<String> {
        self.unavailable.clone()
    }
}

/// A seeded world on its database. Dropped, it stops its tasks and drops
/// the database.
pub struct PgWorld {
    pub in_process: InProcess<Stores>,
    /// The access config the directory was loaded from (the world's,
    /// with the extra operators).
    #[allow(dead_code, reason = "read by the HTTP harness only")]
    pub access: AccessConfig,
    /// The present: the anchor once provisioned.
    #[allow(dead_code, reason = "read by the HTTP harness only")]
    pub clock: Arc<dyn Clock>,
    #[allow(dead_code, reason = "read by the HTTP harness only")]
    pub frame_retention: crosstalk_spec::aggregates::projection::FrameRetention,
    tasks: Vec<JoinHandle<()>>,
    dead_letter_bus: PgBus,
    _db: DbGuard,
}

/// A test database dropped (on the helper runtime) with its guard, also
/// when a provisioning fails half way.
pub struct DbGuard(Option<TestDb>);

impl Drop for DbGuard {
    fn drop(&mut self) {
        if let Some(db) = self.0.take()
            && let Err(error) = on_pg(async move { db.close().await })
        {
            eprintln!("conformance_pg: the test database was left behind: {error}");
        }
    }
}

impl Drop for PgWorld {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        self.dead_letter_bus.shutdown();
    }
}

/// `work` within [`WAIT`], or `Failed` naming `what`.
pub async fn within<T, E: std::fmt::Debug>(
    scenario: &'static str,
    what: &str,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, ProvisionError> {
    match tokio::time::timeout(WAIT, work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(ProvisionError::Failed {
            scenario,
            reason: format!("{what}: {error:?}"),
        }),
        Err(_) => Err(ProvisionError::Failed {
            scenario,
            reason: format!("{what} took over {WAIT:?}"),
        }),
    }
}

/// Rows `topology.outbox` holds: L7 events its relay has not published.
pub async fn topology_outbox(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM topology.outbox")
        .fetch_one(pool)
        .await?;
    Ok(rows)
}

/// Seeds the world into a fresh migrated database and hosts the surface
/// over it (see the module docs), with `extra` operators added to the
/// world's directory; returns it with `request.scenario` bound.
pub async fn provision(
    databases: &Databases,
    request: Provision<'_>,
    extra: Vec<OperatorConfig>,
) -> Result<(PgWorld, Bindings), ProvisionError> {
    let scenario = request.scenario;
    let name = scenario.name();
    let failed = |reason: String| ProvisionError::Failed {
        scenario: name,
        reason,
    };
    let db = databases.database().map_err(failed)?;
    let pool = PgPoolOptions::new()
        .max_connections(POOL)
        .acquire_timeout(Duration::from_secs(30))
        .connect_lazy_with(db.url().connect_options().clone());
    let db = DbGuard(Some(db));
    let world = World::new(SEED, UI_ANCHOR).map_err(|e| failed(e.to_string()))?;
    let config = world.config().clone();
    let access = with_operators(&config.access, extra).map_err(failed)?;
    let clock = Arc::new(SeedClock {
        anchor: world.anchor(),
        serving: OnceLock::new(),
    });
    let reader: Arc<dyn Clock> = clock.clone();
    let secret = Arc::new(KeyedHasher::new(
        DeploymentSecret::from_hex(SecretVersion(1), SECRET_HEX)
            .map_err(|e| failed(format!("{e:?}")))?,
    ));
    let mut live = crosstalk_api::world::WorldOptions::new(SEED, UI_ANCHOR)
        .map_err(|e| failed(e.to_string()))?
        .live;
    if let Some(knob) = request.knobs.live {
        live = knob;
    }
    let export_limits = request.knobs.export.unwrap_or_default();
    let options = InProcessOptions {
        clock: Arc::clone(&reader),
        seed: SEED,
        surface: SurfaceConfig {
            export_formats: ExportFormats::new(vec![ExportFormat::Jsonl])
                .map_err(|e| failed(format!("{e:?}")))?,
            export_limits,
            gateway: GatewayVersion::new(env!("CARGO_PKG_VERSION"))
                .map_err(|e| failed(format!("{e:?}")))?,
            default_remap_threshold: config.rules.default_remap_threshold,
            frame_retention: config.frame_retention,
            live,
        },
        access: access.clone(),
        bucket_width: config.bucket_width,
        timing: config.timing,
        retention: config.retention,
        lineage_floor: config.lineage_floor,
        embedding_model: config.embedding.clone(),
        sinks: config
            .sinks
            .iter()
            .map(|sink| SinkConfig {
                id: sink.id,
                kind: sink.kind,
                name: sink.name.clone(),
            })
            .collect(),
        projection_lease: Duration::from_secs(600),
        projection_fitting: ProjectionFitting::External,
    };
    let (events, published) = unbounded_channel();
    let bus = Forwarding {
        events,
        idle: MpscBus::start(BusConfig::default()).map_err(|e| failed(format!("{e:?}")))?,
    };
    let dead_letter_bus = PgBus::new(
        pool.clone(),
        Arc::new(crosstalk_spec::support::SystemClock),
        PgBusConfig::default(),
    )
    .map_err(|e| failed(format!("{e:?}")))?;
    let (stores, topology_relay) = within(
        name,
        "opening the postgres stores",
        PgStores::open(PgOpen {
            pool: pool.clone(),
            bus: bus.clone(),
            dead_letters: dead_letter_bus.dead_letters(),
            blobs: MemoryBlobStore::new(),
            clock: Arc::clone(&reader),
            secret: Arc::clone(&secret),
            ids: PgIds::Seeded(SEED),
            settings: PgSettings {
                bucket_width: options.bucket_width,
                timing: options.timing,
                retention: options.retention,
                lineage_floor: options.lineage_floor,
                embedding_model: options.embedding_model.clone(),
                default_remap_threshold: options.surface.default_remap_threshold,
                projection_lease: options.projection_lease,
                frame_retention: options.surface.frame_retention.as_duration(),
                sinks: options.sinks.clone(),
                configure_sinks: true,
                threading: Default::default(),
                retry: SerializableRetry::default(),
            },
        }),
    )
    .await?;
    let mut tasks = vec![tokio::spawn(topology_relay.run(
        BusAnnouncer::new(bus.clone()),
        OutboxIds::new(Arc::clone(&reader), SeededRandom::new(SEED ^ 0x70B0)),
        Duration::from_millis(50),
    ))];
    let in_process = within(
        name,
        "hosting the surface",
        InProcess::host(
            stores,
            options,
            published,
            CursorSecret::Derived(&secret),
            "in-process access config",
        ),
    )
    .await?;
    let world_scenario = within(
        name,
        "seeding the world into postgres",
        world.seed(&mut PgSeeding(in_process.stores.clone())),
    )
    .await?;
    within(name, "relaying topology.outbox", async {
        while topology_outbox(&pool).await? > 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<(), sqlx::Error>(())
    })
    .await?;
    within(name, "the relay's settle", in_process.settle()).await?;
    clock.serving.get_or_init(|| world.clock());
    let watermark = within(name, "the watermark", in_process.stores.edges.watermark()).await?;
    tasks.push(spawn_fitter(
        &in_process.stores,
        watermark,
        Arc::clone(&reader),
    ));
    let lead = crosstalk_conformance::harness::caller(OPERATOR_RESEARCHER, PermissionSet::ALL)
        .map_err(|e| failed(e.to_string()))?;
    let present = within(
        name,
        "the surface's watermark",
        in_process.surface.watermark(&lead),
    )
    .await?;
    let stores = &in_process.stores;
    let bindings = bind(
        WorldReads {
            agents: &stores.agents,
            channels: &BindChannels {
                channels: &stores.channels,
                evidence: &stores.evidence,
            },
            transmissions: &stores.transmissions,
            world: &world_scenario,
            watermark: present.at(),
            bucket: BUCKET,
        },
        scenario,
    )
    .await?;
    Ok((
        PgWorld {
            in_process,
            access,
            clock: reader,
            frame_retention: config.frame_retention,
            tasks,
            dead_letter_bus,
            _db: db,
        },
        bindings,
    ))
}

/// The world's access config with `extra` operators added to its
/// authenticated directory.
fn with_operators(
    access: &AccessConfig,
    extra: Vec<OperatorConfig>,
) -> Result<AccessConfig, String> {
    if extra.is_empty() {
        return Ok(access.clone());
    }
    match access {
        AccessConfig::Authenticated(operators) => {
            let mut all = operators.clone();
            all.extend(extra);
            Ok(AccessConfig::Authenticated(all))
        }
        AccessConfig::Trusted(_) => {
            Err("the world's directory is trusted: it takes no other operators".to_owned())
        }
    }
}

/// The world's week and a day before it, to the bucket after the anchor:
/// every harness's extent.
pub fn extent() -> TimeWindow {
    let start: Timestamp = minus(UI_ANCHOR, 8 * DAY);
    let end = plus(UI_ANCHOR, BUCKET.as_micros().get());
    TimeWindow::new(start, end).unwrap_or_else(|_| panic!("the world's extent"))
}

/// Fits queued projection jobs every 20 ms over the Postgres search
/// index's samples with `crosstalk-memory`'s deterministic layout, as the
/// memory harness's `ProjectionFitting::Deterministic` does over its
/// stores (that fitter is the in-process surface's own, for the memory
/// bundle only). The sample is dated by the watermark the world settled
/// at: nothing moves it once provisioned.
pub fn spawn_fitter(
    stores: &Stores,
    watermark: crosstalk_spec::support::Watermark,
    clock: Arc<dyn Clock>,
) -> JoinHandle<()> {
    let mut jobs = stores.projections.clone();
    let source = PgProjectionSource::new(stores.search.clone(), FixedWatermark(watermark));
    tokio::spawn(async move {
        loop {
            if let Err(error) = fit_pass(&mut jobs, &source, &clock).await {
                tracing::warn!(error = %error, "projection fitting pass failed");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
}

/// One pass: requeue lapsed jobs, then fit every queued one.
pub async fn fit_pass<J, S>(jobs: &mut J, source: &S, clock: &Arc<dyn Clock>) -> Result<(), String>
where
    J: ProjectionStore,
    S: ProjectionSource,
{
    let failed =
        |error: crosstalk_spec::interfaces::l6_analysis::ProjectionJobError| format!("{error:?}");
    jobs.requeue_lapsed(clock.now()).await.map_err(failed)?;
    while let Some(job) = jobs.claim(clock.now()).await.map_err(failed)? {
        match fit(&job, source).await {
            Ok(frame) => jobs
                .complete(job.id(), frame, clock.now())
                .await
                .map_err(failed)?,
            Err(Some(failure)) => jobs
                .fail(job.id(), failure, clock.now())
                .await
                .map_err(failed)?,
            // Left to its lease, as the in-process fitter leaves it.
            Err(None) => {}
        }
    }
    Ok(())
}

/// `job`'s frame; `Err(Some)` when the job fails, `Err(None)` when the fit
/// is left to its lease.
pub async fn fit<S: ProjectionSource>(
    job: &ProjectionInfo,
    source: &S,
) -> Result<ProjectionFrame, Option<FitFailure>> {
    let spec = job.spec();
    let sample = match source.sample(spec).await {
        Ok(sample) => sample,
        Err(SampleError::Failed(failure)) => return Err(Some(failure)),
        Err(SampleError::Store { .. }) => return Err(None),
    };
    let embeddings: Vec<Embedding> = sample
        .rows
        .iter()
        .map(|row| row.embedding.clone())
        .collect();
    let coordinates = match FakeLayoutFitter.fit(&embeddings, spec.params()).await {
        Ok(coordinates) => coordinates,
        Err(LayoutError::Failed(failure)) => return Err(Some(failure)),
        Err(LayoutError::Backend { .. }) => return Err(None),
    };
    if coordinates.len() != sample.rows.len() {
        return Err(None);
    }
    let mut points = Vec::with_capacity(sample.rows.len());
    for (row, [x, y]) in sample.rows.into_iter().zip(coordinates) {
        let (Ok(x), Ok(y)) = (Finite::new(x), Finite::new(y)) else {
            return Err(Some(FitFailure::NonFiniteLayout));
        };
        let point = ProjectedPoint::new(PointParts {
            transmission: row.transmission,
            from: row.from,
            to: row.to,
            route: row.route,
            topic: row.topic,
            confirmed_at: row.confirmed_at,
            x,
            y,
        })
        .map_err(|_| None)?;
        points.push(point);
    }
    let header = FrameHeader {
        projection: job.id(),
        topic_version: spec.topic_version(),
        watermark: sample.watermark,
        limit: spec.params().limit(),
        matching: sample.matching,
    };
    ProjectionFrame::from_points(header, &points).map_err(|_| None)
}
