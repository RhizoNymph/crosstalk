//! The template a test process seeds once per access config, and the
//! recording blob store and `WorldStores` bundle the seed writes through
//! (see the parent module's docs for the whole flow).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use crosstalk_api::pg::{
    PgAgentStore, PgAlerts, PgCatalog, PgChannels, PgEdges, PgProjections, PgSearch,
    PgTransmissions,
};
use crosstalk_api::{CursorSecret, InProcess, PgIds, PgStores};
use crosstalk_provenance::store::PgProvenanceStore;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::ids::{MessageHash, SeededRandom};
use crosstalk_spec::interfaces::l2_transport::{BlobError, BlobStore};
use crosstalk_spec::interfaces::l8_surface::operators::AccessConfig;
use crosstalk_spec::support::Clock;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_store::{DatabaseUrl, TemplateDb, TestDb};
use crosstalk_surface::pg::{PgAuditLog, PgOperatorStore, PgSinkRegistry};
use crosstalk_topology::outbox::{BusAnnouncer, OutboxIds};
use crosstalk_transport::PgDeadLetters;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_world::clock::UI_ANCHOR;
use crosstalk_world::{Scenario, World, WorldStores};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::watch;

use super::{
    ACCESS_LABEL, Forwarding, OpenParts, SEED, SeedClock, Stores, TEMPLATE_WAIT, TOPOLOGY_IDS,
    WAIT, bounded, dead_letter_bus, drain_topology_outbox, idle_bus, migrate_all, pg_runtime,
    pool_on, secret, surface_options,
};

/// A seeded, settled world frozen in a template database, with what lives
/// outside Postgres: the bodies, the events the relay applied, and the
/// ids the seed gave each role.
pub struct Template {
    pub db: TemplateDb,
    /// Every body the seed put, in put order.
    pub bodies: Vec<Arc<[u8]>>,
    /// Every event the stores forwarded to the relay, in order.
    pub events: Vec<BusEvent>,
    pub scenario: Scenario,
}

/// A template's seed: `None` while running, then its outcome (an `Err`
/// is every waiting test's failure).
type Seeded = Option<Result<Arc<Template>, String>>;

/// The templates of this process, by access config (its `Debug` form): each
/// seeded once, on the helper runtime, whatever happens to the test that
/// asked first.
fn templates() -> &'static Mutex<BTreeMap<String, watch::Receiver<Seeded>>> {
    static TEMPLATES: OnceLock<Mutex<BTreeMap<String, watch::Receiver<Seeded>>>> = OnceLock::new();
    TEMPLATES.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// The template of the world with `access` as its directory: seeded by the
/// first caller in the process, waited for by every other.
pub async fn template(admin: DatabaseUrl, access: AccessConfig) -> Result<Arc<Template>, String> {
    let key = format!("{access:?}");
    let mut seeded = {
        let mut slots = templates().lock().unwrap_or_else(PoisonError::into_inner);
        slots
            .entry(key)
            .or_insert_with(|| {
                let (done, seeded) = watch::channel(None);
                pg_runtime().spawn(async move {
                    let outcome =
                        match tokio::time::timeout(TEMPLATE_WAIT, seed_template(admin, access))
                            .await
                        {
                            Ok(outcome) => outcome.map(Arc::new),
                            Err(_) => {
                                Err(format!("seeding the template took over {TEMPLATE_WAIT:?}"))
                            }
                        };
                    if let Err(reason) = &outcome {
                        eprintln!("conformance_pg: the template failed: {reason}");
                    }
                    done.send_replace(Some(outcome));
                });
                seeded
            })
            .clone()
    };
    let waited = tokio::time::timeout(TEMPLATE_WAIT + WAIT, seeded.wait_for(Option::is_some)).await;
    match waited {
        Err(_) => Err(format!(
            "waiting for the template took over {:?}",
            TEMPLATE_WAIT + WAIT
        )),
        Ok(Err(_)) => Err("the template's seed stopped without an outcome".to_owned()),
        Ok(Ok(outcome)) => match &*outcome {
            Some(outcome) => outcome.clone(),
            None => Err("the template's seed reported nothing".to_owned()),
        },
    }
}

/// Seeds the world with `access` as its directory into a fresh template
/// database, settles it, closes every connection on it and freezes it
/// (see the module docs). Runs on [`pg_runtime`].
async fn seed_template(admin: DatabaseUrl, access: AccessConfig) -> Result<Template, String> {
    let started = std::time::Instant::now();
    eprintln!("conformance_pg: seeding the template database (once per process)");
    let db = bounded(
        "creating the template database",
        WAIT,
        TestDb::create_template(admin, TestDb::default_pool_settings()),
    )
    .await?;
    bounded("migrating the template", WAIT, migrate_all(&db)).await?;
    let pool = pool_on(db.url());
    let world = World::new(SEED, UI_ANCHOR).map_err(|e| e.to_string())?;
    let clock = Arc::new(SeedClock::new(world.anchor()));
    let reader: Arc<dyn Clock> = clock.clone();
    let secret = secret()?;
    let options = surface_options(world.config(), access, Arc::clone(&reader), None, None)?;
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let (events, published) = unbounded_channel();
    let bus = Forwarding {
        events,
        idle: idle_bus()?,
        recorded: Some(Arc::clone(&recorded)),
    };
    let dead_letter_bus = dead_letter_bus(&pool)?;
    let blobs = RecordingBlobs::default();
    let (stores, topology_relay) = bounded(
        "opening the postgres stores on the template",
        WAIT,
        PgStores::open(
            OpenParts {
                pool: &pool,
                bus: &bus,
                dead_letter_bus: &dead_letter_bus,
                blobs: blobs.stored.clone(),
                clock: &reader,
                secret: &secret,
                ids: PgIds::Seeded(SEED),
                options: &options,
            }
            .open(),
        ),
    )
    .await?;
    let relay = tokio::spawn(topology_relay.run(
        BusAnnouncer::new(bus.clone()),
        OutboxIds::new(Arc::clone(&reader), SeededRandom::new(SEED ^ TOPOLOGY_IDS)),
        Duration::from_millis(50),
    ));
    let in_process = bounded(
        "hosting the surface on the template",
        WAIT,
        InProcess::host(
            stores,
            options,
            published,
            CursorSecret::Derived(&secret),
            ACCESS_LABEL,
        ),
    )
    .await?;
    let scenario = bounded(
        "seeding the world into postgres",
        TEMPLATE_WAIT,
        world.seed(&mut PgSeeding {
            stores: in_process.stores.clone(),
            blobs: blobs.clone(),
        }),
    )
    .await?;
    bounded(
        "relaying the template's topology.outbox",
        TEMPLATE_WAIT,
        drain_topology_outbox(&pool),
    )
    .await?;
    bounded(
        "the template relay's settle",
        TEMPLATE_WAIT,
        in_process.settle(),
    )
    .await?;
    let pending = bounded("reading the outboxes", WAIT, outbox_rows(&pool)).await?;
    if !pending.is_empty() {
        // A clone's stores would publish these again when they open.
        tracing::warn!(outboxes = ?pending, "the settled template still holds outbox rows");
        eprintln!("conformance_pg: the settled template still holds outbox rows: {pending:?}");
    }
    let bodies = blobs.bodies();
    let replayed = MemoryBlobStore::new();
    for body in &bodies {
        replayed.put(body).await.map_err(|e| format!("{e:?}"))?;
    }
    let (held, recorded_count) = (
        in_process
            .stores
            .blobs
            .len()
            .map_err(|e| format!("{e:?}"))?,
        replayed.len().map_err(|e| format!("{e:?}"))?,
    );
    if held != recorded_count {
        return Err(format!(
            "the template's blob store holds {held} bodies, the seed put {recorded_count}: \
             a store put bodies of its own, which the clones would lack"
        ));
    }
    let events = std::mem::take(&mut *recorded.lock().unwrap_or_else(PoisonError::into_inner));
    // Nothing may stay connected to a template: stop everything that holds
    // a connection (the dead-letter bus's LISTEN one included), then close
    // the pool; `freeze` ends whatever is left.
    relay.abort();
    if let Err(error) = relay.await
        && !error.is_cancelled()
    {
        return Err(format!("the template's topology relay failed: {error}"));
    }
    dead_letter_bus.shutdown();
    in_process.shutdown().await;
    drop(bus);
    if tokio::time::timeout(WAIT, pool.close()).await.is_err() {
        tracing::warn!("the template's pool did not close in time; freezing terminates it");
    }
    let frozen = bounded("freezing the template", WAIT, db.freeze()).await?;
    eprintln!(
        "conformance_pg: template {} seeded in {:?} ({} events, {} bodies)",
        frozen.name().map_or("?", |name| name.as_str()),
        started.elapsed(),
        events.len(),
        bodies.len()
    );
    Ok(Template {
        db: frozen,
        bodies,
        events,
        scenario,
    })
}

/// The outboxes still holding rows, with their counts.
async fn outbox_rows(pool: &PgPool) -> Result<Vec<(&'static str, i64)>, sqlx::Error> {
    let mut pending = Vec::new();
    for table in [
        "reconstruct.outbox",
        "flow.outbox",
        "analysis.outbox",
        "topology.outbox",
    ] {
        let (rows,): (i64,) =
            sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {table}")))
                .fetch_one(pool)
                .await?;
        if rows > 0 {
            pending.push((table, rows));
        }
    }
    Ok(pending)
}

/// The seed's blob store: the bundle's [`MemoryBlobStore`], every body put
/// also recorded so each clone's store can be given the same bodies.
#[derive(Clone, Default)]
pub struct RecordingBlobs {
    stored: MemoryBlobStore,
    bodies: Arc<Mutex<Vec<Arc<[u8]>>>>,
}

impl RecordingBlobs {
    /// Every body put so far, in put order (repeats included).
    fn bodies(&self) -> Vec<Arc<[u8]>> {
        self.bodies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl BlobStore for RecordingBlobs {
    async fn put(&self, bytes: &[u8]) -> Result<MessageHash, BlobError> {
        let hash = self.stored.put(bytes).await?;
        self.bodies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::from(bytes));
        Ok(hash)
    }

    async fn get(&self, hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        self.stored.get(hash).await
    }
}

/// The Postgres bundle as the world's [`WorldStores`]: the seed writes
/// through the spec's write traits on handles sharing the surface's pool,
/// and its bodies through [`RecordingBlobs`] over the bundle's blob store.
pub struct PgSeeding {
    pub stores: Stores,
    pub blobs: RecordingBlobs,
}

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
    type Blobs = RecordingBlobs;

    fn agents(&mut self) -> &mut Self::Agents {
        &mut self.stores.agents
    }
    fn spans(&mut self) -> &mut Self::Spans {
        &mut self.stores.provenance
    }
    fn channels(&mut self) -> &mut Self::Channels {
        &mut self.stores.channels
    }
    fn transmissions(&mut self) -> &mut Self::Transmissions {
        &mut self.stores.transmissions
    }
    fn catalog(&mut self) -> &mut Self::Catalog {
        &mut self.stores.catalog
    }
    fn search(&mut self) -> &mut Self::Search {
        &mut self.stores.search
    }
    fn edges(&mut self) -> &mut Self::Edges {
        &mut self.stores.edges
    }
    fn alerts(&mut self) -> &mut Self::Alerts {
        &mut self.stores.alerts
    }
    fn projections(&mut self) -> &mut Self::Projections {
        &mut self.stores.projections
    }
    fn operators(&mut self) -> &mut Self::Operators {
        &mut self.stores.operators
    }
    fn audit(&mut self) -> &mut Self::Audit {
        &mut self.stores.audit
    }
    fn sinks(&mut self) -> &mut Self::Sinks {
        &mut self.stores.sinks
    }
    fn letters(&mut self) -> &mut Self::Letters {
        &mut self.stores.dead_letters
    }
    fn blobs(&mut self) -> &mut Self::Blobs {
        &mut self.blobs
    }
}
