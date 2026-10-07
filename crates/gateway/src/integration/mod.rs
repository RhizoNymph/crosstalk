//! Postgres integration tests of the gateway's Postgres mode: migrations,
//! the head check, the pipeline lock, recovery, the frontier and the
//! persisted watermark. Each runs on a fresh database from
//! `TestDb::new_or_skip` (the node0 server named by `TEST_DATABASE_URL`)
//! and skips, passing, without one.

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
use crate::live::pg::{PgParts, PgSet};
use crate::live::recovery::{LockState, MigrationState, PipelinePhase, RecoveryStep};
use crate::live::{Live, LiveClock, LiveConfig, Slot};
use crate::ops::{Ops, PgOps, Phase};
use crate::role::Role;
use crate::spool::{Gate, Gated};
use crate::store::lock::PipelineLock;
use crate::store::migrations::{LAYERS, check_heads, migrate_all};
use crate::store::{StoreProbe, lazy_pool};
use crate::tasks::Tasks;

const SECRET_HEX: &str = "5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2";
const PATIENCE: Duration = Duration::from_secs(60);
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
    let task = postgres::spawn_pipeline(start);

    let behind = tokio::time::timeout(
        PATIENCE,
        status.wait_until(|status| matches!(status.migrations, MigrationState::Behind(_))),
    )
    .await
    .expect("the head check reports")
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
    let running = tokio::time::timeout(
        PATIENCE,
        status.wait_until(|status| status.phase == PipelinePhase::Running),
    )
    .await
    .expect("the pipeline starts")
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

    if let Some((live, lock)) = task_stop(task).await {
        live.shutdown(Instant::now() + Duration::from_secs(5)).await;
        drop(lock);
    }
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
    let task = postgres::spawn_pipeline(start);
    tokio::time::timeout(
        PATIENCE,
        status.wait_until(|status| status.lock == LockState::HeldElsewhere),
    )
    .await
    .expect("the second process sees the lock")
    .expect("the reporter lives");
    let ready = ops.readiness().await;
    assert!(!ready.ready, "{ready:?}");
    assert_eq!(ready.pipeline_lock.as_deref(), Some("held elsewhere"));
    assert_eq!(ready.pipeline.as_deref(), Some("waiting_for_lock"));

    drop(first);
    tokio::time::timeout(
        PATIENCE,
        status.wait_until(|status| status.phase == PipelinePhase::Running),
    )
    .await
    .expect("the second process takes over")
    .expect("the reporter lives");
    if let Some((live, lock)) = task_stop(task).await {
        live.shutdown(Instant::now() + Duration::from_secs(5)).await;
        drop(lock);
    }
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
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::at(Timestamp::from_micros(T0)));
    let bus = PgBus::new(pipeline_pool(&db), Arc::clone(&clock), bus_config()).expect("a bus");
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
    let delivery = tokio::time::timeout(PATIENCE, inside.next())
        .await
        .expect("delivered")
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
    let second = tokio::time::timeout(PATIENCE, inside.next())
        .await
        .expect("delivered")
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
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::at(Timestamp::from_micros(T0)));
    let bus = PgBus::new(pipeline_pool(&db), Arc::clone(&clock), bus_config()).expect("a bus");
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
    let deadline = Instant::now() + PATIENCE;
    while spool.stats().records > 0 {
        assert!(Instant::now() < deadline, "the spool drains");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
    let start = |clock: ManualClock, spool: &str| {
        let pool = pipeline_pool(&db);
        let spool = spool_config(&dir.path().join(spool));
        async move {
            let parts = PgParts::open(
                pool,
                Arc::new(clock.clone()),
                bus_config(),
                spool,
                secret(),
                PgIds::Seeded(0xE2E),
            )
            .await
            .expect("the parts");
            Live::<PgSet>::start_pg(test_live_config(clock), parts)
                .await
                .expect("the live process starts")
        }
    };
    let live = start(clock.clone(), "first").await;
    assert_eq!(live.report().watermark_micros, 0);
    let settled = live
        .settle(Timestamp::from_micros(T0 + 3_600_000_000))
        .await
        .expect("settles");
    let before = live.report().watermark_micros;
    assert!(before > 0, "the watermark advanced by {settled:?}");
    live.shutdown(Instant::now() + Duration::from_secs(10))
        .await;

    let restarted = start(clock, "restarted").await;
    assert_eq!(
        restarted.report().watermark_micros,
        before,
        "the persisted watermark, from the first report"
    );
    restarted
        .shutdown(Instant::now() + Duration::from_secs(10))
        .await;
    db.close().await.expect("drops");
}
