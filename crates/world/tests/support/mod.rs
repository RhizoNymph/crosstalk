//! The memory stores as [`WorldStores`], and one world seeded into them,
//! shared by every test of a test binary.
//!
//! Every test runs its body on one current-thread runtime kept for the
//! binary's life (the in-process bus that holds the dead letters is a task
//! on it), one test at a time.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

pub mod read;

use crosstalk_memory::analysis::alerts::{AlertStoreConfig, InMemoryAlertStore};
use crosstalk_memory::analysis::catalog::{CatalogConfig, InMemoryTopicCatalog};
use crosstalk_memory::analysis::projection::{InMemoryProjectionStore, ProjectionConfig};
use crosstalk_memory::analysis::search::InMemorySearchIndex;
use crosstalk_memory::flow::{MemoryChannels, MemoryVerdicts};
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_memory::support::{IdSequence, Outbox};
use crosstalk_memory::surface::audit::InMemoryAuditLog;
use crosstalk_memory::surface::operators::InMemoryOperatorStore;
use crosstalk_memory::surface::sinks::{InMemorySinkRegistry, SinkConfig};
use crosstalk_memory::topology::env::{Env, StaticNodes};
use crosstalk_memory::topology::store::{EdgeStoreConfig, InMemoryEdgeStore};
use crosstalk_spec::ids::{AgentId, ChannelId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l5_flow::ChannelDirectory;
use crosstalk_spec::support::Timestamp;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, DeadLetters, MpscBus};
use crosstalk_world::{Scenario, World, WorldEmbedder, WorldError, WorldStores};
use tokio::runtime::Runtime;

/// The seed every shared world is generated from.
pub const SEED: u64 = 0x005e_edc0_ffee;

/// Both directories over the memory L3 and L5 stores: what the L6 and L7
/// stores resolve merges and supersessions through.
#[derive(Clone)]
pub struct Directories {
    pub agents: MemoryAgents,
    pub channels: MemoryChannels<MemoryAgents>,
}

impl AgentDirectory for Directories {
    fn canonical(&self, id: AgentId) -> AgentId {
        AgentDirectory::canonical(&self.agents, id)
    }
}

impl ChannelDirectory for Directories {
    fn canonical(&self, id: ChannelId) -> ChannelId {
        ChannelDirectory::canonical(&self.channels, id)
    }
}

pub type Edges = InMemoryEdgeStore<Env<InMemoryTopicCatalog, Directories, StaticNodes>>;

/// One memory store per role, configured from a world's config.
#[derive(Clone)]
pub struct MemoryWorld {
    pub agents: MemoryAgents,
    pub channels: MemoryChannels<MemoryAgents>,
    pub transmissions: MemoryVerdicts,
    pub catalog: InMemoryTopicCatalog,
    pub search: InMemorySearchIndex<Directories>,
    pub edges: Edges,
    pub alerts: InMemoryAlertStore<WorldEmbedder, Directories>,
    pub projections: InMemoryProjectionStore,
    pub operators: InMemoryOperatorStore,
    pub audit: InMemoryAuditLog,
    pub sinks: InMemorySinkRegistry,
    pub letters: DeadLetters,
    pub blobs: MemoryBlobStore,
    pub bus: MpscBus,
}

impl MemoryWorld {
    /// Empty stores configured from `world`'s config. Starts the bus, so
    /// it must be called on a runtime.
    pub fn new(world: &World) -> Result<Self, String> {
        let config = world.config();
        let agents = MemoryAgents::new(IdSequence::default(), Outbox::none());
        let channels = MemoryChannels::new(agents.clone(), IdSequence::default(), Outbox::none());
        let directories = Directories {
            agents: agents.clone(),
            channels: channels.clone(),
        };
        let transmissions = MemoryVerdicts::new(Outbox::none());
        let catalog = InMemoryTopicCatalog::new(
            CatalogConfig {
                retention: config.retention,
                lineage_floor: config.lineage_floor,
            },
            world.anchor().config_at(),
            Outbox::none(),
        )
        .map_err(|e| format!("{e:?}"))?;
        let search = InMemorySearchIndex::new(
            catalog.clone(),
            directories.clone(),
            config.embedding.clone(),
        );
        let edges = InMemoryEdgeStore::new(
            EdgeStoreConfig {
                bucket_width: config.bucket_width,
                timing: config.timing,
            },
            Env {
                topics: catalog.clone(),
                directory: directories.clone(),
                nodes: StaticNodes::new(),
            },
            Outbox::none(),
        );
        let alerts = InMemoryAlertStore::new(
            AlertStoreConfig {
                rules: config.rules,
                sinks: config.sinks.iter().map(|s| s.id).collect::<BTreeSet<_>>(),
                builtins: config
                    .builtins
                    .iter()
                    .map(|b| (b.rule, (b.status, b.sinks.clone())))
                    .collect::<BTreeMap<_, _>>(),
            },
            world.embedder(),
            directories,
            transmissions.clone(),
            Outbox::none(),
        );
        let projections = InMemoryProjectionStore::new(
            ProjectionConfig {
                lease: Duration::from_secs(600),
                frame_retention: config.frame_retention.as_duration(),
            },
            Outbox::none(),
        );
        let audit = InMemoryAuditLog::new();
        let operators = InMemoryOperatorStore::new(audit.clone(), IdSequence::default());
        let sinks = InMemorySinkRegistry::new(config.sinks.iter().map(|s| SinkConfig {
            id: s.id,
            kind: s.kind,
            name: s.name.clone(),
        }));
        let bus = MpscBus::start(BusConfig::default()).map_err(|e| format!("{e:?}"))?;
        Ok(Self {
            agents,
            channels,
            transmissions,
            catalog,
            search,
            edges,
            alerts,
            projections,
            operators,
            audit,
            sinks,
            letters: bus.dead_letters(),
            blobs: MemoryBlobStore::new(),
            bus,
        })
    }
}

impl WorldStores for MemoryWorld {
    type Agents = MemoryAgents;
    type Channels = MemoryChannels<MemoryAgents>;
    type Transmissions = MemoryVerdicts;
    type Catalog = InMemoryTopicCatalog;
    type Search = InMemorySearchIndex<Directories>;
    type Edges = Edges;
    type Alerts = InMemoryAlertStore<WorldEmbedder, Directories>;
    type Projections = InMemoryProjectionStore;
    type Operators = InMemoryOperatorStore;
    type Audit = InMemoryAuditLog;
    type Sinks = InMemorySinkRegistry;
    type Letters = DeadLetters;
    type Blobs = MemoryBlobStore;

    fn agents(&mut self) -> &mut Self::Agents {
        &mut self.agents
    }
    fn channels(&mut self) -> &mut Self::Channels {
        &mut self.channels
    }
    fn transmissions(&mut self) -> &mut Self::Transmissions {
        &mut self.transmissions
    }
    fn catalog(&mut self) -> &mut Self::Catalog {
        &mut self.catalog
    }
    fn search(&mut self) -> &mut Self::Search {
        &mut self.search
    }
    fn edges(&mut self) -> &mut Self::Edges {
        &mut self.edges
    }
    fn alerts(&mut self) -> &mut Self::Alerts {
        &mut self.alerts
    }
    fn projections(&mut self) -> &mut Self::Projections {
        &mut self.projections
    }
    fn operators(&mut self) -> &mut Self::Operators {
        &mut self.operators
    }
    fn audit(&mut self) -> &mut Self::Audit {
        &mut self.audit
    }
    fn sinks(&mut self) -> &mut Self::Sinks {
        &mut self.sinks
    }
    fn letters(&mut self) -> &mut Self::Letters {
        &mut self.letters
    }
    fn blobs(&mut self) -> &mut Self::Blobs {
        &mut self.blobs
    }
}

/// A world seeded into memory stores.
pub struct Seeded {
    pub world: World,
    pub stores: MemoryWorld,
    pub scenario: Scenario,
}

/// Seeds the world of `seed` anchored at the UI's anchor into fresh memory
/// stores. Must run on [`runtime`].
pub async fn seed(seed: u64) -> Result<Seeded, WorldError> {
    seed_at(seed, crosstalk_world::UI_ANCHOR).await
}

pub async fn seed_at(seed: u64, at: Timestamp) -> Result<Seeded, WorldError> {
    let world = World::new(seed, at)?;
    let mut stores = MemoryWorld::new(&world).map_err(WorldError::Missing)?;
    let scenario = world.seed(&mut stores).await?;
    Ok(Seeded {
        world,
        stores,
        scenario,
    })
}

/// The binary's one runtime, taken by one test at a time.
fn runtime() -> &'static Mutex<Runtime> {
    static RUNTIME: OnceLock<Mutex<Runtime>> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        #[allow(clippy::expect_used)]
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime");
        Mutex::new(runtime)
    })
}

/// Runs `body` on the binary's runtime.
pub fn run<T>(body: impl Future<Output = T>) -> T {
    let runtime = runtime()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    runtime.block_on(body)
}

/// The world of [`SEED`], seeded once per test binary.
pub fn shared() -> &'static Seeded {
    static SHARED: OnceLock<Seeded> = OnceLock::new();
    SHARED.get_or_init(|| {
        #[allow(clippy::expect_used)]
        run(seed(SEED)).expect("the shared world seeds")
    })
}
