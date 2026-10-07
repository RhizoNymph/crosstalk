//! Postgres integration tests of the gateway's Postgres mode: migrations,
//! the head check, the pipeline lock, recovery, the frontier and the
//! persisted watermark. Each runs on a fresh database from
//! `TestDb::new_or_skip` (the node0 server named by `TEST_DATABASE_URL`)
//! and skips, passing, without one.

use std::future::Future;
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::PgIds;
use crosstalk_flow::consumer::FlowConfig;
use crosstalk_flow::store::PgShardTicks;
use crosstalk_memory::support::ManualClock;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, DeploymentSecret, EventId, KeyedHasher, SecretVersion};
use crosstalk_spec::interfaces::l2_transport::{
    ConsumerGroup, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::interfaces::l7_topology::FrontierSource;
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_store::sqlx::PgPool;
use crosstalk_store::{PoolSettings, StoreConfig, TestDb};
use crosstalk_transport::{NonZeroDuration, PgBus, PgBusConfig, SpoolConfig, SpoolingBus};
use tokio::time::Instant;

use crate::config::GatewayConfig;
use crate::gateway::postgres::{self, Late, PipelineStart};
use crate::live::frontier::PgFrontierSource;
use crate::live::pg::diagnose::{DiagnoseFrom, PgDiagnosis, bounded, diagnose};
use crate::live::pg::{PgParts, PgSet, bus_clock};
use crate::live::recovery::{LockState, MigrationState, PipelinePhase, RecoveryStep, StatusReader};
use crate::live::{Live, LiveClock, LiveConfig, Slot};
use crate::ops::{Ops, PgOps, Phase};
use crate::role::Role;
use crate::spool::{Gate, Gated};
use crate::store::lock::PipelineLock;
use crate::store::migrations::{LAYERS, check_heads, migrate_all};
use crate::store::{StoreProbe, lazy_pool};
use crate::tasks::Tasks;

const SECRET_HEX: &str = "5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2";
/// How long a wait on the database (recovery, a settle, a delivery, the
/// lock, LISTEN) may take before the test fails with what the process was
/// waiting on: minutes, for a loaded LAN Postgres.
const WAIT: Duration = Duration::from_secs(300);

/// What a stalled wait's diagnosis reads.
struct Probe {
    pool: PgPool,
    bus: PgBus,
    spool: Option<SpoolingBus<Gated>>,
    status: Option<StatusReader>,
}

impl Probe {
    async fn diagnosis(&self) -> PgDiagnosis {
        let groups: Vec<ConsumerGroup> = [
            Slot::L3Reconstruct,
            Slot::L4Provenance,
            Slot::L5Flow,
            Slot::L6Classify,
            Slot::L7Topology,
        ]
        .iter()
        .map(|slot| slot.group())
        .collect();
        diagnose(DiagnoseFrom {
            pool: &self.pool,
            bus: &self.bus,
            spool: self.spool.as_ref(),
            groups: &groups,
            shards: NonZeroU16::MIN,
            status: self.status.as_ref(),
            watermark: None,
        })
        .await
    }
}

/// `work` within [`WAIT`], or a panic naming `what` and what the process
/// was waiting on.
async fn within<T>(what: &str, work: impl Future<Output = T>, probe: &Probe) -> T {
    bounded(WAIT, what, work, || probe.diagnosis())
        .await
        .unwrap_or_else(|stalled| panic!("{stalled}"))
}

fn start_probe(start: &PipelineStart) -> Probe {
    Probe {
        pool: start.pool.clone(),
        bus: start.bus.clone(),
        spool: Some(start.spool.clone()),
        status: Some(start.status.reader()),
    }
}

/// Stop the pipeline task and its live process, bounded.
async fn stop_pipeline(task: postgres::StopHandle<postgres::PipelineHeld>, probe: &Probe) {
    let held = within("stopping the pipeline task", task_stop(task), probe).await;
    if let Some((live, lock)) = held {
        within(
            "the live process's shutdown",
            live.shutdown(Instant::now() + Duration::from_secs(30)),
            probe,
        )
        .await;
        drop(lock);
    }
}
const T0: u64 = 1_790_845_200_000_000;

fn secret() -> Arc<KeyedHasher> {
    let secret =
        DeploymentSecret::from_hex(SecretVersion(1), SECRET_HEX).expect("a valid test secret");
    Arc::new(KeyedHasher::new(secret))
}

/// A fresh database, or `None` (skipped) without a test server.
async fn database(test: &str) -> Option<TestDb> {
    TestDb::new_or_skip(test).await.expect("the test database")
}

async fn migrated(test: &str) -> Option<TestDb> {
    let db = database(test).await?;
    migrate_all(&db.store())
        .await
        .expect("every layer migrates");
    Some(db)
}

/// A pool of its own on the test database, large enough for a pipeline.
fn pipeline_pool(db: &TestDb) -> PgPool {
    let settings = PoolSettings::new(
        NonZeroU32::MIN.saturating_add(11),
        0,
        Duration::from_secs(30),
    )
    .expect("pool settings");
    lazy_pool(&StoreConfig::new(db.url().clone(), settings))
}

fn bus_config() -> PgBusConfig {
    PgBusConfig {
        poll: NonZeroDuration::new(Duration::from_millis(20)).expect("non-zero"),
        ..PgBusConfig::default()
    }
}

fn spool_config(dir: &std::path::Path) -> SpoolConfig {
    SpoolConfig::with_limits(
        dir.join("spool"),
        std::num::NonZeroU64::new(1 << 24).expect("non-zero"),
        std::num::NonZeroU64::new(1 << 20).expect("non-zero"),
        std::num::NonZeroUsize::new(64).expect("non-zero"),
        NonZeroDuration::new(Duration::from_millis(10)).expect("non-zero"),
    )
    .expect("spool bounds")
}

fn envelope(id: u128, at: Timestamp) -> Envelope {
    Envelope {
        id: EventId::from_ulid(id),
        at,
        event: BusEvent::Changed(Changed::Agent(AgentId::from_ulid(id))),
    }
}

/// `crosstalk migrate` runs every layer, `transport` first, and running it
/// again changes nothing; before it, every layer is behind.
#[tokio::test(flavor = "multi_thread")]
async fn pg_migrate_runs_every_layer_and_is_idempotent() {
    let Some(db) = database("pg_migrate_runs_every_layer_and_is_idempotent").await else {
        return;
    };
    let behind = check_heads(db.pool()).await.expect("reads the heads");
    assert_eq!(
        behind.iter().map(|behind| behind.layer).collect::<Vec<_>>(),
        LAYERS.iter().map(|layer| layer.layer).collect::<Vec<_>>(),
        "a fresh database is behind in every layer"
    );
    assert!(behind.iter().all(|behind| behind.applied == 0));
    migrate_all(&db.store()).await.expect("migrates");
    assert_eq!(check_heads(db.pool()).await.expect("reads"), Vec::new());
    migrate_all(&db.store()).await.expect("migrates again");
    assert_eq!(check_heads(db.pool()).await.expect("reads"), Vec::new());
    db.close().await.expect("drops");
}

/// The pipeline task's start for a test gateway config on `db`.
async fn pipeline_start(
    db: &TestDb,
    data: &std::path::Path,
    late: Late,
) -> (PipelineStart, Gate, SpoolingBus<Gated>, PgBus) {
    let config = GatewayConfig::from_json(
        &serde_json::json!({
            "ingress": {
                "listen": "127.0.0.1:0",
                "routes": [],
                "secrets": {"current": {"version": 1, "env": "CROSSTALK_SECRET_V1"}},
            },
            "ops": {"listen": "127.0.0.1:0"},
            "store": {"pool": {"max_connections": 12}, "bus": {"poll_micros": 20_000}},
            "spool": {"probe_ms": 10},
            "blobs": {"root": data.join("blobs")},
        })
        .to_string(),
    )
    .expect("the config parses");
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::at(Timestamp::from_micros(T0)));
    let blobs = crosstalk_transport::blob::FsBlobStore::open(&config.blobs.root)
        .await
        .expect("blobs");
    let side = postgres::capture_side(&config, Role::Pipeline, pipeline_pool(db), &blobs, &clock)
        .await
        .expect("the capture side");
    let spool = side.spool.clone().expect("a pipeline role spools");
    let start = PipelineStart {
        config: config.clone(),
        role: Role::Pipeline,
        clock: LiveClock::Read(Arc::clone(&clock)),
        blobs,
        pool: side.pool.clone(),
        url: db.url().clone(),
        bus: side.bus.clone(),
        spool: spool.clone(),
        gate: side.gate.clone(),
        pipeline: side.pipeline.clone().expect("a capture pipeline"),
        secret: secret(),
        status: postgres::initial_status(),
        late,
        api: None,
    };
    (start, side.gate, spool, side.bus)
}

fn ops(start: &PipelineStart, spool: SpoolingBus<Gated>, late: &Late) -> Ops {
    let (_, phase) = tokio::sync::watch::channel(Phase::Ok);
    Ops {
        role: Role::Pipeline,
        phase,
        capture: None,
        pipeline: Arc::clone(start.pipeline.stats()),
        log: Arc::new(crate::log::consumer::LogStats::new()),
        live: None,
        tasks: Tasks::new(),
        store: StoreProbe::not_configured(),
        postgres: Some(PgOps {
            status: start.status.reader(),
            bus: start.bus.clone(),
            spool: Some(spool),
            live: Arc::clone(&late.reporter),
            clock: Arc::new(ManualClock::at(Timestamp::from_micros(T0))),
        }),
    }
}

/// `/readyz` on an unmigrated database: not ready, `migrations: behind:`
/// naming the layers; once migrated, the pipeline walks every recovery
/// step and runs, ready, with the lock held and recovery done.
#[tokio::test(flavor = "multi_thread")]
async fn pg_readyz_reports_behind_then_every_recovery_step() {
    let Some(db) = database("pg_readyz_reports_behind_then_every_recovery_step").await else {
        return;
    };
    let dir = tempfile::tempdir().expect("a temp dir");
    let late = Late::default();
    let (start, _gate, spool, _bus) = pipeline_start(&db, dir.path(), late.clone()).await;
    let ops = ops(&start, spool, &late);
    let mut status = start.status.reader();
    let probe = start_probe(&start);
    let task = postgres::spawn_pipeline(start);

    let behind = within(
        "the head check",
        status.wait_until(|status| matches!(status.migrations, MigrationState::Behind(_))),
        &probe,
    )
    .await
    .expect("the reporter lives");
    assert_eq!(behind.phase, PipelinePhase::WaitingForMigrations);
    let ready = ops.readiness().await;
    assert!(!ready.ready, "{ready:?}");
    assert!(
        ready.migrations.starts_with("behind: transport 0 < 1"),
        "{}",
        ready.migrations
    );
    assert_eq!(ready.pipeline.as_deref(), Some("waiting_for_migrations"));

    migrate_all(&db.store()).await.expect("migrates");
    let running = within(
        "the pipeline's recovery",
        status.wait_until(|status| status.phase == PipelinePhase::Running),
        &probe,
    )
    .await
    .expect("the reporter lives");
    assert_eq!(running.migrations, MigrationState::AtHead);
    assert_eq!(running.lock, LockState::Held);
    assert_eq!(
        running.steps,
        [
            RecoveryStep::RecoveringBus,
            RecoveryStep::RelayingOutboxes,
            RecoveryStep::RestoringFlow,
            RecoveryStep::RebuildingNodes,
            RecoveryStep::Subscribing,
        ]
    );
    let ready = ops.readiness().await;
    assert!(ready.ready, "{ready:?}");
    assert_eq!(ready.recovery.as_deref(), Some("done"));
    assert_eq!(ready.pipeline_lock.as_deref(), Some("held"));
    assert_eq!(ready.migrations, "at_head");

    stop_pipeline(task, &probe).await;
    db.close().await.expect("drops");
}

async fn task_stop(task: postgres::StopHandle<postgres::PipelineHeld>) -> postgres::PipelineHeld {
    task.stop().await.flatten()
}

/// One pipeline process per database: a second one waits, `pipeline_lock:
/// held elsewhere` and not ready, and takes over once the first lets go.
#[tokio::test(flavor = "multi_thread")]
async fn pg_second_pipeline_waits_while_the_lock_is_held_elsewhere() {
    let Some(db) = migrated("pg_second_pipeline_waits_while_the_lock_is_held_elsewhere").await
    else {
        return;
    };
    let first = PipelineLock::try_take(db.url())
        .await
        .expect("connects")
        .expect("the first process takes the lock");
    assert!(
        PipelineLock::try_take(db.url())
            .await
            .expect("connects")
            .is_none(),
        "a second session cannot take it"
    );
    let dir = tempfile::tempdir().expect("a temp dir");
    let late = Late::default();
    let (start, _gate, spool, _bus) = pipeline_start(&db, dir.path(), late.clone()).await;
    let ops = ops(&start, spool, &late);
    let mut status = start.status.reader();
    let probe = start_probe(&start);
    let task = postgres::spawn_pipeline(start);
    within(
        "the second process seeing the lock",
        status.wait_until(|status| status.lock == LockState::HeldElsewhere),
        &probe,
    )
    .await
    .expect("the reporter lives");
    let ready = ops.readiness().await;
    assert!(!ready.ready, "{ready:?}");
    assert_eq!(ready.pipeline_lock.as_deref(), Some("held elsewhere"));
    assert_eq!(ready.pipeline.as_deref(), Some("waiting_for_lock"));

    drop(first);
    within(
        "the second process taking over",
        status.wait_until(|status| status.phase == PipelinePhase::Running),
        &probe,
    )
    .await
    .expect("the reporter lives");
    stop_pipeline(task, &probe).await;
    db.close().await.expect("drops");
}

/// `topology.frontier.covers-pending` (INV-581): a pipeline group's
/// pending delivery and a dead letter hold `oldest_pending` at their `at`;
/// a group outside the pipeline does not count; acked, nothing is pending.
#[tokio::test(flavor = "multi_thread")]
async fn pg_frontier_covers_pending_deliveries() {
    let Some(db) = migrated("pg_frontier_covers_pending_deliveries").await else {
        return;
    };
    let bus = PgBus::new(pipeline_pool(&db), bus_clock(), bus_config()).expect("a bus");
    let probe = Probe {
        pool: db.pool().clone(),
        bus: bus.clone(),
        spool: None,
        status: None,
    };
    let shards = NonZeroU16::MIN;
    let pipeline = Slot::L3Reconstruct.group();
    let outside = ConsumerGroup("outside".to_owned());
    let frontier = PgFrontierSource::new(
        bus.clone(),
        db.pool().clone(),
        PgShardTicks::new(db.pool().clone()),
        shards,
        vec![pipeline.clone()],
        (),
    );
    let retry = RetryPolicy::new(
        NonZeroU32::MIN,
        Duration::from_millis(1),
        Duration::from_millis(1),
    )
    .expect("a retry policy");
    let mut inside = bus
        .subscribe(&[Subject::Changed], pipeline, retry)
        .await
        .expect("subscribes");
    let _outside = bus
        .subscribe(&[Subject::Changed], outside, retry)
        .await
        .expect("subscribes");
    let empty = frontier.frontier().await.expect("reads");
    assert_eq!(empty.oldest_pending, None);
    assert_eq!(
        empty.ticked_through,
        Timestamp::from_micros(0),
        "no shard ticked"
    );

    let at = Timestamp::from_micros(T0 - 60_000_000);
    bus.publish(envelope(1, at)).await.expect("publishes");
    let delivery = within("the first delivery", inside.next(), &probe)
        .await
        .expect("open")
        .expect("decodes");
    assert_eq!(
        frontier.frontier().await.expect("reads").oldest_pending,
        Some(at)
    );
    // One attempt: a nack dead-letters it, and the dead letter holds.
    inside
        .nack(delivery.id, Duration::from_millis(1), "test".to_owned())
        .await
        .expect("nacks");
    assert_eq!(
        frontier.frontier().await.expect("reads").oldest_pending,
        Some(at)
    );

    let later = Timestamp::from_micros(T0);
    bus.publish(envelope(2, later)).await.expect("publishes");
    let second = within("the second delivery", inside.next(), &probe)
        .await
        .expect("open")
        .expect("decodes");
    inside.ack(second.id).await.expect("acks");
    assert_eq!(
        frontier.frontier().await.expect("reads").oldest_pending,
        Some(at),
        "the dead letter still holds; the outside group's backlog never counts"
    );
    bus.shutdown();
    db.close().await.expect("drops");
}

/// `topology.frontier.covers-spool` (INV-1217) over the real spool: an
/// envelope spooled behind the closed gate bounds `oldest_pending`; once
/// the gate opens and the spool drains into a pipeline group, the group's
/// pending delivery holds it instead.
#[tokio::test(flavor = "multi_thread")]
async fn pg_frontier_covers_the_spool() {
    let Some(db) = migrated("pg_frontier_covers_the_spool").await else {
        return;
    };
    let dir = tempfile::tempdir().expect("a temp dir");
    let bus = PgBus::new(pipeline_pool(&db), bus_clock(), bus_config()).expect("a bus");
    let gate = Gate::closed();
    let spool = SpoolingBus::open(
        Gated::new(bus.clone(), gate.clone()),
        spool_config(dir.path()),
    )
    .await
    .expect("the spool opens");
    let group = Slot::L3Reconstruct.group();
    let frontier = PgFrontierSource::new(
        bus.clone(),
        db.pool().clone(),
        PgShardTicks::new(db.pool().clone()),
        NonZeroU16::MIN,
        vec![group.clone()],
        spool.clone(),
    );
    let retry = RetryPolicy::new(
        NonZeroU32::MIN.saturating_add(4),
        Duration::from_millis(1),
        Duration::from_millis(1),
    )
    .expect("a retry policy");
    let _subscription = spool
        .subscribe(&[Subject::Changed], group, retry)
        .await
        .expect("subscribes");
    let at = Timestamp::from_micros(T0 - 120_000_000);
    spool.publish(envelope(7, at)).await.expect("spooled");
    assert_eq!(spool.stats().records, 1);
    assert_eq!(
        frontier.frontier().await.expect("reads").oldest_pending,
        Some(at)
    );
    gate.open();
    let probe = Probe {
        pool: db.pool().clone(),
        bus: bus.clone(),
        spool: Some(spool.clone()),
        status: None,
    };
    within(
        "the spool's drain",
        async {
            while spool.stats().records > 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        &probe,
    )
    .await;
    assert_eq!(
        frontier.frontier().await.expect("reads").oldest_pending,
        Some(at),
        "drained into the log, not yet admitted by the group: still pending"
    );
    spool.close().await;
    bus.shutdown();
    db.close().await.expect("drops");
}

/// The live process's config for a Postgres-mode test run on `clock`.
pub(crate) fn test_live_config(clock: ManualClock) -> LiveConfig {
    let flow = FlowConfig {
        checkpoint_ms: 1_000,
        ..FlowConfig::default()
    };
    LiveConfig::new(LiveClock::Manual(clock), flow, 0xE2E).expect("the defaults")
}

/// `/healthz`'s `live.watermark_micros` is the persisted watermark from
/// the first report after a restart, never lower than before it.
#[tokio::test(flavor = "multi_thread")]
async fn pg_watermark_survives_a_restart() {
    let Some(db) = migrated("pg_watermark_survives_a_restart").await else {
        return;
    };
    let dir = tempfile::tempdir().expect("a temp dir");
    let clock = ManualClock::at(Timestamp::from_micros(T0));
    let live = start_live(&db, &dir.path().join("first"), clock.clone()).await;
    assert_eq!(live.report().watermark_micros, 0);
    let settled = settle_live(&live, Timestamp::from_micros(T0 + 3_600_000_000)).await;
    let before = live.report().watermark_micros;
    assert!(before > 0, "the watermark advanced by {settled:?}");
    stop_live(live).await;

    let restarted = start_live(&db, &dir.path().join("restarted"), clock).await;
    assert_eq!(
        restarted.report().watermark_micros,
        before,
        "the persisted watermark, from the first report"
    );
    stop_live(restarted).await;
    db.close().await.expect("drops");
}

/// The restart e2e hang's regression: a delivery a stage retries (an
/// `ExchangeCaptured` whose bodies were never stored: L3 cannot read them)
/// is nacked, delayed, retried and finally dead-lettered while a settle
/// waits, although the live process runs on a manual clock that the
/// settle does not move. The bus times its delays on the wall clock
/// (`live::pg::bus_clock`); on the manual clock the delayed delivery
/// would never come due and the settle would wait forever.
#[tokio::test(flavor = "multi_thread")]
async fn pg_settle_finishes_while_a_delivery_is_retried() {
    let Some(db) = migrated("pg_settle_finishes_while_a_delivery_is_retried").await else {
        return;
    };
    let dir = tempfile::tempdir().expect("a temp dir");
    let clock = ManualClock::at(Timestamp::from_micros(T0));
    let live = start_live(&db, &dir.path().join("spool"), clock).await;
    let mut ids = crosstalk_testkit::ids::Ids::seeded(5);
    let exchange = crosstalk_testkit::build::ExchangeBuilder::new(&mut ids).build();
    let envelope = Envelope {
        id: EventId::from_ulid(0xE7),
        at: Timestamp::from_micros(T0),
        event: BusEvent::Ingest(
            crosstalk_spec::events::ingest::IngestEvent::ExchangeCaptured(Box::new(exchange)),
        ),
    };
    live.stores()
        .bus
        .publish(envelope)
        .await
        .expect("publishes");
    settle_live(&live, Timestamp::from_micros(T0 + 1_000_000)).await;
    let stats = live.layers().bus.group_stats().await.expect("group stats");
    let l3 = stats
        .iter()
        .find(|stats| stats.group == Slot::L3Reconstruct.group())
        .expect("the L3 group");
    assert_eq!(l3.pending, 0, "{l3:?}");
    assert_eq!(l3.dead_letters, 1, "retried, then dead-lettered: {l3:?}");
    stop_live(live).await;
    db.close().await.expect("drops");
}

/// A Postgres-mode live process over `db` on `clock`, its spool in
/// `spool`; recovery bounded by [`WAIT`].
async fn start_live(db: &TestDb, spool: &std::path::Path, clock: ManualClock) -> Live<PgSet> {
    let parts = PgParts::open(
        pipeline_pool(db),
        bus_config(),
        spool_config(spool),
        secret(),
        PgIds::Seeded(0xE2E),
    )
    .await
    .expect("the parts");
    let probe = parts.clone();
    bounded(
        WAIT,
        "the live process's recovery",
        Live::<PgSet>::start_pg(test_live_config(clock), parts),
        || async move { probe.diagnose(NonZeroU16::MIN).await },
    )
    .await
    .unwrap_or_else(|stalled| panic!("{stalled}"))
    .expect("the live process starts")
}

/// `live.settle(until)` bounded by [`WAIT`].
async fn settle_live(live: &Live<PgSet>, until: Timestamp) -> crate::live::Settled {
    bounded(WAIT, "a settle", live.settle(until), || live.diagnose())
        .await
        .unwrap_or_else(|stalled| panic!("{stalled}"))
        .expect("settles")
}

/// Shut `live` down and close its spool, bounded by [`WAIT`].
async fn stop_live(live: Live<PgSet>) {
    let spool = live.layers().spool.clone();
    let probe = Probe {
        pool: live.layers().pool.clone(),
        bus: live.layers().bus.clone(),
        spool: Some(spool.clone()),
        status: Some(live.layers().status.clone()),
    };
    within(
        "the live process's shutdown",
        live.shutdown(Instant::now() + Duration::from_secs(30)),
        &probe,
    )
    .await;
    within("closing the spool", spool.close(), &probe).await;
}
