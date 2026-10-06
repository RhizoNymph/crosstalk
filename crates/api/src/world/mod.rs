//! A surface over the synthetic world, for tests and demos (feature
//! `world`): the in-process surface's memory stores seeded with
//! `crosstalk-world` through the spec's write traits ([`seed_world`]), and
//! the same surface served over HTTP on a loopback port ([`serve_world`]).
//!
//! ```text
//! seed_world(options)
//!   World::new(seed, anchor)          config, clock, embedder
//!   InProcess::start_with_reads(options from the world's config, standalone backbone, WorldLayers)
//!   World::seed_with_wire(&mut Seeding(stores))  every write, in time order → Scenario, Wire
//!   conversations::record(wire)       each exchange through L1, L3 threading and L4 provenance
//!                                     into WorldLayers (the conversation reads' stores)
//!   InProcess::settle                 the node facts have applied every seeded event
//!   InProcess::fit_projections        when options.projection_fitting asks for it
//!   clock: config time while seeding ─▶ the anchor (Fixed) or the anchor plus real time (Live)
//! serve_world(world, tokens)
//!   HttpApi::new(surface, Auth::fixed(directory, StaticTokens(tokens)))
//!   bind 127.0.0.1:0, serve until shutdown
//! ```
//!
//! The operator UI's world backend and the L8 conformance suite's harnesses
//! (in process and over `crosstalk-client`) start the world this way.
//!
//! The conversation reads (`conversations`, `conversation_turns`,
//! `span_points`, …) answer from [`WorldLayers`]: the world's wire traffic
//! threaded and scanned by the same L3 and L4 code the live gateway runs
//! (see [`record`]).

mod conversations;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crosstalk_memory::analysis::catalog::InMemoryTopicCatalog;
use crosstalk_memory::analysis::projection::InMemoryProjectionStore;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::surface::audit::InMemoryAuditLog;
use crosstalk_memory::surface::operators::InMemoryOperatorStore;
use crosstalk_memory::surface::sinks::{InMemorySinkRegistry, SinkConfig};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::export::{
    ExportFormat, ExportFormats, ExportLimits, GatewayVersion,
};
use crosstalk_spec::interfaces::l8_surface::live::LiveConfig;
use crosstalk_spec::interfaces::l8_surface::operators::{
    AccessConfig, OperatorConfig, OperatorDirectory,
};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_surface::SurfaceConfig;
use crosstalk_transport::DeadLetters;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_world::clock::{DAY, MINUTE, minus};
use crosstalk_world::{Anchor, WireScope, World, WorldClock, WorldError, WorldStores};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::http::{Auth, BearerToken, HttpApi, HttpConfig, ServeError, StaticTokens, bind, serve};
use crate::in_process::{
    Alerts, Backbone, ConversationStores, Edges, InProcess, InProcessError, InProcessOptions,
    MemoryEvidence, MemoryStores, ProjectionFitting, Search,
};

pub use conversations::{CaptureBlobs, RecordError, Recorded, WorldLayers, record};

/// The world's stores: the reference stores, the conversation reads over
/// [`WorldLayers`].
pub type WorldMemoryStores = MemoryStores<MemoryBlobStore, WorldLayers>;
/// The in-process surface a seeded world runs.
pub type WorldInProcess = InProcess<MemoryBlobStore, WorldLayers>;
/// The surface over a seeded world's stores.
pub type WorldSurface = crosstalk_surface::Surface<WorldMemoryStores>;

/// The in-process surface's memory stores as the world's [`WorldStores`]:
/// the seed writes through the spec's write traits on handles that share
/// state with the surface.
pub struct Seeding<R = WorldLayers>(pub MemoryStores<MemoryBlobStore, R>);

impl<R: ConversationStores> WorldStores for Seeding<R> {
    type Agents = MemoryAgents;
    type Spans = MemoryEvidence;
    type Channels = MemoryChannels<MemoryAgents>;
    type Transmissions = MemoryVerdicts;
    type Catalog = InMemoryTopicCatalog;
    type Search = Search;
    type Edges = Edges;
    type Alerts = Alerts;
    type Projections = InMemoryProjectionStore;
    type Operators = InMemoryOperatorStore;
    type Audit = InMemoryAuditLog;
    type Sinks = InMemorySinkRegistry;
    type Letters = DeadLetters;
    type Blobs = MemoryBlobStore;

    fn agents(&mut self) -> &mut Self::Agents {
        &mut self.0.agents
    }
    fn spans(&mut self) -> &mut Self::Spans {
        &mut self.0.evidence
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

/// How the present moves once the world is seeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldTime {
    /// Stays at the anchor: every action is stamped there (tests).
    Fixed,
    /// Moves on from the anchor in real time (serving).
    Live,
}

/// Whether the world's exchanges are threaded (L3) and scanned (L4) into
/// the conversation reads when it is seeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldConversations {
    /// The wire traffic of the transmissions `WireScope` holds is recorded.
    /// Recording costs about 2 ms an exchange in a release build (the
    /// whole week is about 14,500 exchanges), several times that in a debug
    /// one.
    Recorded(WireScope),
    /// Nothing is recorded: the conversation reads answer empty.
    Unrecorded,
}

/// What to seed and how to serve it.
#[derive(Debug, Clone)]
pub struct WorldOptions {
    pub seed: u64,
    /// The world's present: its data ends here.
    pub anchor: Timestamp,
    pub time: WorldTime,
    pub live: LiveConfig,
    pub export_limits: ExportLimits,
    /// Operators added to the world's own (the researcher and the on-call
    /// operator) in the surface's directory.
    pub operators: Vec<OperatorConfig>,
    /// Whether projection jobs queued once the world is seeded are fitted
    /// in this process. The seed's own jobs are written as planned either
    /// way: the fitter starts after seeding, so it then also fits the
    /// world's queued job.
    pub projection_fitting: ProjectionFitting,
    /// Which of the world's exchanges the conversation reads hold.
    pub conversations: WorldConversations,
}

impl WorldOptions {
    /// The world of `seed` at `anchor` on a fixed clock: a feed buffer of
    /// 256 with a 15-second heartbeat and ten minutes' retention, the
    /// default export limits, no extra operators, projection jobs left for
    /// an external fitter, and the conversations of the last day's
    /// transmissions (and of those retention dropped a body of) recorded.
    pub fn new(seed: u64, anchor: Timestamp) -> Result<Self, WorldServeError> {
        let live = LiveConfig::new(
            std::num::NonZeroU32::new(256).unwrap_or(std::num::NonZeroU32::MIN),
            Duration::from_secs(15),
            Duration::from_secs(600),
        )
        .map_err(|e| WorldServeError::option("live", e))?;
        Ok(Self {
            seed,
            anchor,
            time: WorldTime::Fixed,
            live,
            export_limits: ExportLimits::default(),
            operators: Vec::new(),
            projection_fitting: ProjectionFitting::External,
            conversations: WorldConversations::Recorded(WireScope::Since(minus(anchor, DAY))),
        })
    }
}

/// Why the world could not be seeded or served.
#[derive(Debug, thiserror::Error)]
pub enum WorldServeError {
    #[error("the world could not be generated or seeded: {0}")]
    World(#[from] WorldError),
    #[error("the in-process surface could not start: {0}")]
    Surface(#[from] InProcessError),
    #[error("the world's conversations could not be recorded: {0}")]
    Conversations(Box<RecordError>),
    #[error("the API could not listen: {0}")]
    Serve(#[from] ServeError),
    #[error("invalid option {what}: {reason}")]
    Option { what: &'static str, reason: String },
}

impl From<RecordError> for WorldServeError {
    fn from(error: RecordError) -> Self {
        Self::Conversations(Box::new(error))
    }
}

impl WorldServeError {
    fn option(what: &'static str, reason: impl std::fmt::Debug) -> Self {
        Self::Option {
            what,
            reason: format!("{reason:?}"),
        }
    }
}

/// The present the surface and its stores read: just before the world's
/// first config load while the world is seeded (every seeded write carries
/// its own time), then the anchor, fixed or moving on.
#[derive(Debug)]
pub struct SeedClock {
    anchor: Anchor,
    time: WorldTime,
    serving: OnceLock<WorldClock>,
}

impl SeedClock {
    fn new(anchor: Anchor, time: WorldTime) -> Self {
        Self {
            anchor,
            time,
            serving: OnceLock::new(),
        }
    }

    /// From now on the present is the anchor's.
    fn serve(&self) {
        let clock = match self.time {
            WorldTime::Fixed => WorldClock::Fixed(self.anchor),
            WorldTime::Live => WorldClock::live(self.anchor),
        };
        // Set once, after seeding; a second call keeps the first clock.
        let _ = self.serving.set(clock);
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

/// A seeded world: the in-process surface over its stores, the handles of
/// what the world holds, and the access config its directory was loaded
/// from.
pub struct SeededWorld {
    pub in_process: WorldInProcess,
    pub scenario: crosstalk_world::Scenario,
    /// What threading the world's exchanges did.
    pub recorded: Recorded,
    pub access: AccessConfig,
    pub clock: Arc<SeedClock>,
    pub frame_retention: crosstalk_spec::aggregates::projection::FrameRetention,
}

/// Starts the in-process surface over empty memory stores configured from
/// the world of `options.seed` at `options.anchor`, seeds the world into
/// them, records its exchanges' conversations (L1, L3, L4) and starts the
/// clock.
pub async fn seed_world(options: WorldOptions) -> Result<SeededWorld, WorldServeError> {
    let scope = match options.conversations {
        WorldConversations::Recorded(scope) => scope,
        // Nothing is recorded; the wire is still generated, so keep it small.
        WorldConversations::Unrecorded => WireScope::Since(Timestamp::from_micros(u64::MAX)),
    };
    let world = World::new(options.seed, options.anchor)?.with_wire(scope);
    let config = world.config();
    let clock = Arc::new(SeedClock::new(world.anchor(), options.time));
    let export_formats = ExportFormats::new(vec![ExportFormat::Jsonl])
        .map_err(|e| WorldServeError::option("export formats", e))?;
    let gateway = GatewayVersion::new(env!("CARGO_PKG_VERSION"))
        .map_err(|e| WorldServeError::option("gateway version", e))?;
    let access = with_operators(&config.access, options.operators)?;
    let in_process_options = InProcessOptions {
        clock: clock.clone(),
        seed: options.seed,
        surface: SurfaceConfig {
            export_formats,
            export_limits: options.export_limits,
            gateway,
            default_remap_threshold: config.rules.default_remap_threshold,
            frame_retention: config.frame_retention,
            live: options.live,
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
        // Started once seeded: the seed claims and settles its own jobs.
        projection_fitting: ProjectionFitting::External,
    };
    let layers = WorldLayers::new();
    let mut in_process =
        InProcess::start_with_reads(in_process_options, Backbone::standalone()?, layers.clone())
            .await?;
    let (scenario, wire) = world
        .seed_with_wire(&mut Seeding(in_process.stores.clone()))
        .await?;
    // Threaded after the seed: clusters are read from the agent store as
    // the seed left it (its merges applied).
    let recorded = match options.conversations {
        WorldConversations::Recorded(_) => {
            conversations::record(
                wire,
                &in_process.stores.blobs,
                &in_process.stores.agents,
                &layers,
                clock.clone(),
                options.seed,
            )
            .await?
        }
        WorldConversations::Unrecorded => Recorded::default(),
    };
    // The graphs' node facts follow the stores through the relay: a world
    // is read only once the relay has applied everything the seed wrote.
    in_process.settle().await?;
    clock.serve();
    in_process.fit_projections(options.projection_fitting);
    tracing::info!(
        seed = options.seed,
        "world seeded into the in-process surface"
    );
    Ok(SeededWorld {
        in_process,
        scenario,
        recorded,
        access,
        clock,
        frame_retention: config.frame_retention,
    })
}

/// The world's access config with `extra` operators added to an
/// authenticated directory (a trusted one takes none).
fn with_operators(
    access: &AccessConfig,
    extra: Vec<OperatorConfig>,
) -> Result<AccessConfig, WorldServeError> {
    if extra.is_empty() {
        return Ok(access.clone());
    }
    match access {
        AccessConfig::Authenticated(operators) => {
            let mut all = operators.clone();
            all.extend(extra);
            Ok(AccessConfig::Authenticated(all))
        }
        AccessConfig::Trusted(_) => Err(WorldServeError::option(
            "operators",
            "the world's directory is trusted: it takes no other operators",
        )),
    }
}

/// A seeded world served over HTTP on a loopback port.
pub struct HttpWorld {
    pub world: SeededWorld,
    pub addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), ServeError>>,
}

impl HttpWorld {
    /// `http://127.0.0.1:<port>`: the client's base URL.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Stops serving, waits for the requests in flight, and stops the
    /// surface.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.task.await;
        self.world.in_process.shutdown().await;
    }
}

/// Serves `world`'s surface over HTTP on `127.0.0.1` at a free port, each
/// bearer token of `tokens` authenticating its operator in the world's
/// directory.
pub async fn serve_world(
    world: SeededWorld,
    tokens: Vec<(BearerToken, OperatorId)>,
) -> Result<HttpWorld, WorldServeError> {
    let (directory, _) = OperatorDirectory::load(None, &world.access)
        .map_err(|e| WorldServeError::option("access", e))?;
    let auth = Auth::fixed(directory, StaticTokens::new(tokens));
    let config = HttpConfig {
        frame_retention: world.frame_retention,
        clock: world.clock.clone(),
    };
    let router = HttpApi::new(Arc::clone(&world.in_process.surface), auth, config).router();
    let listener = bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
    let addr = listener.local_addr().map_err(|source| ServeError::Bind {
        addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        source,
    })?;
    let (shutdown, stop) = oneshot::channel::<()>();
    let task = tokio::spawn(serve(listener, router, async move {
        let _ = stop.await;
    }));
    Ok(HttpWorld {
        world,
        addr,
        shutdown,
        task,
    })
}
