//! A seeded synthetic world on the Postgres stores, for the L8
//! conformance suite's Postgres harnesses: in process
//! (`crates/api/tests/conformance_pg.rs`) and over HTTP
//! (`crates/client/tests/conformance_pg.rs`, which includes this file by
//! path).
//!
//! Seeding the world through the Postgres stores is tens of thousands of
//! serializable transactions, so a test process seeds each distinct world
//! (one per access config: the world's directory plus the harness's
//! operators) once, into a template database, and every test gets its own
//! `CREATE DATABASE … TEMPLATE` clone of it:
//!
//! ```text
//! Databases::new        reads the test server's URL (crosstalk_store::test_server);
//!                       none configured: every test skips (Harness::unavailable)
//! template(access)      once per process and access config (TEMPLATES), on the helper
//!                       runtime, bounded by TEMPLATE_WAIT; concurrent tests wait on it
//!   TestDb::create_template (stale templates dropped), every layer's migrations,
//!     the extensions
//!   PgStores::open (PgIds::Seeded(SEED)); bus = Forwarding, recording every event
//!     it forwards to the surface's relay channel
//!   InProcess::host (operators loaded, node facts, CursorSecret::Derived)
//!   World::seed(&mut PgSeeding)   every seeded write through Postgres, in the memory
//!                                 harness's order (surface hosted first); bodies to a
//!                                 MemoryBlobStore through RecordingBlobs
//!   L7's outbox relay runs; wait until topology.outbox is empty; settle
//!   shut the surface, the relay and the bus down, close the pool, TestDb::freeze
//!   -> Template { TemplateDb, the recorded bodies, the recorded events, the Scenario }
//! provision(scenario)   per test, every step bounded by WAIT
//!   TemplateDb::instantiate: a fresh database cloned from the template
//!   a MemoryBlobStore holding the template's bodies
//!   PgStores::open (PgIds::Seeded(SEED ^ CLONE_IDS)): the ids the template's
//!     generators minted are in the clone, so the clone's draw from another seed
//!   InProcess::host with the test's knobs (live feed, export limits)
//!   the template's recorded events replayed, in order, into the relay channel
//!   wait until topology.outbox is empty; settle; the clock to the anchor
//!   projection fitter over PgProjectionSource (FakeLayoutFitter, as in memory),
//!     its FixedWatermark read from the clone
//!   bind the scenario from the clone's reads and the template's Scenario
//! ```
//!
//! A clone ends as the seeded world used to after its settle: the same
//! rows, the same bodies, the same events applied to the node facts and
//! the live feed (whose epoch is the clock's reading at host, before the
//! anchor, as before), the clock serving the anchor. Each test's database
//! is dropped with its world (or with the half-built world when
//! provisioning fails). The template lives as long as the process and is
//! dropped by the next run's `TestDb::create_template` (or the manual
//! `crosstalk_test_%` cleanup).

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use crosstalk_analysis::search::PgProjectionSource;
use crosstalk_api::pg::PgChannels;
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
use crosstalk_spec::interfaces::l2_transport::{
    BlobStore, BusError, ConsumerGroup, EventBus, RetryPolicy,
};
use crosstalk_spec::interfaces::l5_flow::channels::{
    AccessReadError, AccessStore, ChannelReads, ChannelWithTraffic,
};
use crosstalk_spec::interfaces::l5_flow::{ChannelDirectory, RegistryError};
use crosstalk_spec::interfaces::l6_analysis::{
    LayoutError, LayoutFitter, ProjectionSource, ProjectionStore, SampleError,
};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_spec::interfaces::l8_surface::channel_traffic::ChannelTransmissionFilter;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportFormat, ExportFormats, ExportLimits, GatewayVersion,
};
use crosstalk_spec::interfaces::l8_surface::lists::ChannelFilter;
use crosstalk_spec::interfaces::l8_surface::live::LiveConfig;
use crosstalk_spec::interfaces::l8_surface::operators::{AccessConfig, OperatorConfig};
use crosstalk_spec::interfaces::l8_surface::{PermissionSet, QueryApi};
use crosstalk_spec::paging::{ChannelList, ChannelTransmissionList, Page, PageRequest};
use crosstalk_spec::support::{Clock, Finite, TimeWindow, Timestamp};
use crosstalk_store::sqlx::postgres::PgPoolOptions;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_store::{DatabaseUrl, Layer, Migrations, SerializableRetry, StoreError, TestDb};
use crosstalk_surface::{EvidenceRecords, SurfaceConfig};
use crosstalk_topology::outbox::{BusAnnouncer, OutboxIds};
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus, PgBus, PgBusConfig};
use crosstalk_world::clock::{BUCKET, DAY, MINUTE, UI_ANCHOR, minus, plus};
use crosstalk_world::config::{OPERATOR_RESEARCHER, WorldConfig};
use crosstalk_world::{Anchor, World, WorldClock};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;

#[path = "pg_world/template.rs"]
mod template;

/// The seed every world is generated from (the memory harness's).
pub const SEED: u64 = 7;

/// How long any one step of a test's provisioning (cloning the template,
/// opening, hosting, settling) or of the template's setup (creating,
/// migrating, freezing) may take before it fails with its name.
pub const WAIT: Duration = Duration::from_secs(300);

/// How long seeding the template may take, all of it: tens of thousands
/// of serializable transactions against a possibly remote server. Tests
/// waiting on it wait this long (and [`WAIT`] more) too.
pub const TEMPLATE_WAIT: Duration = Duration::from_secs(2 * 60 * 60);

/// The deployment secret the cursor keys derive from.
pub const SECRET_HEX: &str = "c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0c0f0";

/// How many connections each world's pool holds.
pub const POOL: u32 = 8;

/// Mixed into a clone's id seeds (`SEED ^ CLONE_IDS`): the template's
/// generators already minted `SEED`'s first ids into the rows a clone
/// copies, so a clone's generators start elsewhere, as a restarted
/// deployment's do. The high bit keeps it apart from every `seed ^
/// purpose` the stores derive.
pub const CLONE_IDS: u64 = 0xC10E_0000_0000_0000;

/// Mixed into the id seed of L7's outbox relay.
pub const TOPOLOGY_IDS: u64 = 0x70B0;

/// The label the access config's entries are hashed from.
pub const ACCESS_LABEL: &str = "in-process access config";

/// The bus the stores publish to: every event goes to the surface's relay
/// channel, as the memory stores' outbox does (the harness runs no
/// pipeline, so nothing else consumes them). Subscriptions are an idle
/// in-memory bus's. While a template is seeded, every forwarded event is
/// also recorded, in forwarding order, for its clones to replay.
#[derive(Clone)]
pub struct Forwarding {
    events: UnboundedSender<BusEvent>,
    idle: MpscBus,
    recorded: Option<Arc<Mutex<Vec<BusEvent>>>>,
}

impl EventBus for Forwarding {
    type Subscription = <MpscBus as EventBus>::Subscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        let Some(recorded) = &self.recorded else {
            return self
                .events
                .send(envelope.event)
                .map_err(|_| BusError::Disconnected);
        };
        // Held across the send so the record's order is the channel's.
        let mut log = recorded.lock().unwrap_or_else(PoisonError::into_inner);
        self.events
            .send(envelope.event.clone())
            .map_err(|_| BusError::Disconnected)?;
        log.push(envelope.event);
        Ok(())
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
/// carries its own time) and while a clone replays the seed's events, the
/// anchor once served (as `seed_world`'s).
#[derive(Debug)]
pub struct SeedClock {
    anchor: Anchor,
    serving: OnceLock<WorldClock>,
}

impl SeedClock {
    fn new(anchor: Anchor) -> Self {
        Self {
            anchor,
            serving: OnceLock::new(),
        }
    }
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
/// only on one, the suite drives each test on a current-thread one, and
/// the template is seeded here whichever test asked first.
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

/// `work` spawned on [`pg_runtime`] and awaited from the caller's runtime,
/// which keeps running meanwhile.
pub async fn spawned_on_pg<F>(work: F) -> Result<F::Output, String>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    pg_runtime()
        .spawn(work)
        .await
        .map_err(|error| format!("the helper runtime's work failed: {error}"))
}

/// Every layer's migrations and the extensions, into `db`.
async fn migrate_all(db: &TestDb) -> Result<(), StoreError> {
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
    Ok(())
}

/// The test server the worlds' databases live on. Built per test (the
/// suite builds a harness per test); connects to nothing.
pub struct Databases {
    /// The admin URL; `None` when no test server is configured; `Err`
    /// when one is configured but unusable.
    server: Result<Option<DatabaseUrl>, String>,
}

impl Databases {
    pub fn new() -> Self {
        Self {
            server: crosstalk_store::test_server().map_err(|error| error.to_string()),
        }
    }

    /// The server's admin URL, or why there is none to use.
    fn server(&self) -> Result<DatabaseUrl, String> {
        match &self.server {
            Ok(Some(url)) => Ok(url.clone()),
            Ok(None) => Err("TEST_DATABASE_URL is not set".to_owned()),
            Err(reason) => Err(reason.clone()),
        }
    }

    /// Why no world can be provisioned here: no test server configured.
    pub fn unavailable(&self) -> Option<String> {
        match &self.server {
            Ok(None) => Some("TEST_DATABASE_URL is not set".to_owned()),
            Ok(Some(_)) | Err(_) => None,
        }
    }
}

/// `work` within `wait`, or why not, naming `what`.
async fn bounded<T, E: std::fmt::Debug>(
    what: &str,
    wait: Duration,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    match tokio::time::timeout(wait, work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(format!("{what}: {error:?}")),
        Err(_) => Err(format!("{what} took over {wait:?}")),
    }
}

/// The deployment secret's hasher.
fn secret() -> Result<Arc<KeyedHasher>, String> {
    Ok(Arc::new(KeyedHasher::new(
        DeploymentSecret::from_hex(SecretVersion(1), SECRET_HEX).map_err(|e| format!("{e:?}"))?,
    )))
}

/// A lazy pool of [`POOL`] connections on `url`.
fn pool_on(url: &DatabaseUrl) -> PgPool {
    PgPoolOptions::new()
        .max_connections(POOL)
        .acquire_timeout(Duration::from_secs(30))
        .connect_lazy_with(url.connect_options().clone())
}

/// The surface's options for `config`'s world with `access` as its
/// directory, the knobs applied (`None`: the world's defaults).
fn surface_options(
    config: &WorldConfig,
    access: AccessConfig,
    clock: Arc<dyn Clock>,
    live: Option<LiveConfig>,
    export: Option<ExportLimits>,
) -> Result<InProcessOptions, String> {
    let live = match live {
        Some(knob) => knob,
        None => {
            crosstalk_api::world::WorldOptions::new(SEED, UI_ANCHOR)
                .map_err(|e| e.to_string())?
                .live
        }
    };
    Ok(InProcessOptions {
        clock,
        seed: SEED,
        surface: SurfaceConfig {
            export_formats: ExportFormats::new(vec![ExportFormat::Jsonl])
                .map_err(|e| format!("{e:?}"))?,
            export_limits: export.unwrap_or_default(),
            gateway: GatewayVersion::new(env!("CARGO_PKG_VERSION"))
                .map_err(|e| format!("{e:?}"))?,
            default_remap_threshold: config.rules.default_remap_threshold,
            frame_retention: config.frame_retention,
            live,
        },
        access,
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
    })
}

/// What a world's stores open from: `options`' settings, ids drawn from
/// `ids`.
struct OpenParts<'a> {
    pool: &'a PgPool,
    bus: &'a Forwarding,
    dead_letter_bus: &'a PgBus,
    blobs: MemoryBlobStore,
    clock: &'a Arc<dyn Clock>,
    secret: &'a Arc<KeyedHasher>,
    ids: PgIds,
    options: &'a InProcessOptions,
}

impl OpenParts<'_> {
    fn open(self) -> PgOpen<Forwarding, MemoryBlobStore> {
        let options = self.options;
        PgOpen {
            pool: self.pool.clone(),
            bus: self.bus.clone(),
            dead_letters: self.dead_letter_bus.dead_letters(),
            blobs: self.blobs,
            clock: Arc::clone(self.clock),
            secret: Arc::clone(self.secret),
            ids: self.ids,
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
        }
    }
}

/// The idle in-memory bus behind a [`Forwarding`]'s subscriptions.
fn idle_bus() -> Result<MpscBus, String> {
    MpscBus::start(BusConfig::default()).map_err(|e| format!("{e:?}"))
}

/// The bus whose dead letters the stores keep, over `pool`.
fn dead_letter_bus(pool: &PgPool) -> Result<PgBus, String> {
    PgBus::new(
        pool.clone(),
        Arc::new(crosstalk_spec::support::SystemClock),
        PgBusConfig::default(),
    )
    .map_err(|e| format!("{e:?}"))
}

/// Waits until `topology.outbox` is empty: L7's relay published every
/// event it holds.
async fn drain_topology_outbox(pool: &PgPool) -> Result<(), sqlx::Error> {
    while topology_outbox(pool).await? > 0 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
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
    bounded(what, WAIT, work)
        .await
        .map_err(|reason| ProvisionError::Failed { scenario, reason })
}

/// Rows `topology.outbox` holds: L7 events its relay has not published.
pub async fn topology_outbox(pool: &PgPool) -> Result<i64, sqlx::Error> {
    let (rows,): (i64,) = sqlx::query_as("SELECT count(*) FROM topology.outbox")
        .fetch_one(pool)
        .await?;
    Ok(rows)
}

/// Hosts the surface over a fresh clone of the template of the world with
/// `extra` operators added to its directory (seeding that template first
/// if no test in the process has), with `request`'s knobs (see the module
/// docs); returns it with `request.scenario` bound.
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
    let admin = databases.server().map_err(failed)?;
    let world = World::new(SEED, UI_ANCHOR).map_err(|e| failed(e.to_string()))?;
    let config = world.config().clone();
    let access = with_operators(&config.access, extra).map_err(failed)?;
    let template = template::template(admin, access.clone())
        .await
        .map_err(|reason| failed(format!("the template: {reason}")))?;
    let source = Arc::clone(&template);
    let db = within(
        name,
        "cloning the template",
        spawned_on_pg(async move { source.db.instantiate().await }),
    )
    .await?
    .map_err(|error| failed(format!("cloning the template: {error}")))?;
    let pool = pool_on(db.url());
    let db = DbGuard(Some(db));
    let clock = Arc::new(SeedClock::new(world.anchor()));
    let reader: Arc<dyn Clock> = clock.clone();
    let secret = secret().map_err(failed)?;
    let options = surface_options(
        &config,
        access.clone(),
        Arc::clone(&reader),
        request.knobs.live,
        request.knobs.export,
    )
    .map_err(failed)?;
    let (events, published) = unbounded_channel();
    let bus = Forwarding {
        events: events.clone(),
        idle: idle_bus().map_err(failed)?,
        recorded: None,
    };
    let dead_letter_bus = dead_letter_bus(&pool).map_err(failed)?;
    let blobs = MemoryBlobStore::new();
    for body in &template.bodies {
        blobs
            .put(body)
            .await
            .map_err(|e| failed(format!("restoring the template's bodies: {e:?}")))?;
    }
    let (stores, topology_relay) = within(
        name,
        "opening the postgres stores",
        PgStores::open(
            OpenParts {
                pool: &pool,
                bus: &bus,
                dead_letter_bus: &dead_letter_bus,
                blobs,
                clock: &reader,
                secret: &secret,
                ids: PgIds::Seeded(SEED ^ CLONE_IDS),
                options: &options,
            }
            .open(),
        ),
    )
    .await?;
    let mut tasks = vec![tokio::spawn(topology_relay.run(
        BusAnnouncer::new(bus.clone()),
        OutboxIds::new(
            Arc::clone(&reader),
            SeededRandom::new(SEED ^ CLONE_IDS ^ TOPOLOGY_IDS),
        ),
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
            ACCESS_LABEL,
        ),
    )
    .await?;
    // The relay applies what the seed published, in the seed's order, as
    // it did while the template was seeded.
    for event in &template.events {
        events
            .send(event.clone())
            .map_err(|_| failed("replaying the seed's events: the relay is gone".to_owned()))?;
    }
    drop(events);
    within(
        name,
        "relaying topology.outbox",
        drain_topology_outbox(&pool),
    )
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
            world: &template.scenario,
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
