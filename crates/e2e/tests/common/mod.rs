//! What the Postgres-mode end-to-end tests share: a `Live` process over a
//! test database (`Live::<PgSet>::start_pg`), stopped gracefully or killed,
//! restarted over the same database, filesystem blob store and spool
//! directory; every wait bounded by [`WAIT`] with the process's diagnosis
//! (`live::pg::diagnose`); and the uninterrupted memory-mode run whose
//! answers each restarted run must give.
//!
//! Each test binary includes this module (`#[path]`) and uses part of it.
#![allow(dead_code)]

pub mod gateway;

use std::future::Future;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::PgIds;
use crosstalk_e2e::answers::{Answers, answers};
use crosstalk_e2e::scenario::Scenario;
use crosstalk_e2e::{compose_with, options};
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_gateway::live::pg::diagnose::{PgDiagnosis, bounded};
use crosstalk_gateway::live::pg::{PgParts, PgPartsError, PgSet};
use crosstalk_gateway::live::{BlobConfig, Live, LiveClock, LiveConfig, Ticking};
use crosstalk_gateway::pipeline::Settings;
use crosstalk_gateway::store::lazy_pool;
use crosstalk_gateway::store::migrations::migrate_all;
use crosstalk_memory::support::ManualClock;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_reconstruct::thread::ThreadConfig;
use crosstalk_spec::ids::{DeploymentSecret, KeyedHasher, SecretVersion};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::RequestIdentity;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_store::{PoolSettings, StoreConfig, TestDb};
use crosstalk_transport::{BusConfig, NonZeroDuration, PgBusConfig, SpoolConfig, SpoolError};
use tokio::time::Instant;

pub const SECRET_HEX: &str = "5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2";

/// The seed of a run's first process; a restarted one draws from
/// `SEED + generation`, as a restarted `serve` draws new entropy
/// (`surface.ids.unique-across-restart`).
pub const SEED: u64 = 0xE2E;

/// How long a graceful shutdown may drain.
pub const DRAIN: Duration = Duration::from_secs(30);

/// How long any wait on Postgres-mode work (recovery, feeding, a settle,
/// the reads, a shutdown, a kill) may take before the test fails with what
/// the process was waiting on: minutes, for a loaded LAN Postgres.
pub const WAIT: Duration = Duration::from_secs(300);

/// One hour past the scenario's end: every window closed, every
/// suspicion expired or confirmed.
pub const AFTER: u64 = 3_600_000_000;

/// How often a polling wait looks again.
pub const POLL: Duration = Duration::from_millis(20);

/// The four store outboxes' staged rows (what `PgQuiet` counts).
pub const OUTBOX_ROWS: &str = "SELECT (SELECT count(*) FROM reconstruct.outbox) \
     + (SELECT count(*) FROM flow.outbox) \
     + (SELECT count(*) FROM analysis.outbox) \
     + (SELECT count(*) FROM topology.outbox)";

/// Deliveries any group holds, ready, held or delayed.
pub const PENDING_DELIVERIES: &str = "SELECT count(*) FROM transport.deliveries";

pub fn secret() -> Arc<KeyedHasher> {
    let secret =
        DeploymentSecret::from_hex(SecretVersion(1), SECRET_HEX).expect("a valid test secret");
    Arc::new(KeyedHasher::new(secret))
}

/// A fresh migrated database, or `None` (skipped) without a test server.
pub async fn migrated(test: &str) -> Option<TestDb> {
    let db = TestDb::new_or_skip(test)
        .await
        .expect("the test database")?;
    migrate_all(&db.store())
        .await
        .expect("every layer migrates");
    Some(db)
}

pub fn bus_config() -> PgBusConfig {
    PgBusConfig {
        poll: NonZeroDuration::new(Duration::from_millis(20)).expect("non-zero"),
        ..PgBusConfig::default()
    }
}

/// The spool in `<dir>/spool`.
pub fn spool_config(dir: &Path) -> SpoolConfig {
    SpoolConfig::with_limits(
        dir.join("spool"),
        NonZeroU64::new(1 << 26).expect("non-zero"),
        NonZeroU64::new(1 << 22).expect("non-zero"),
        NonZeroUsize::new(64).expect("non-zero"),
        NonZeroDuration::new(Duration::from_millis(10)).expect("non-zero"),
    )
    .expect("spool bounds")
}

/// The composition's config (`crosstalk_e2e::compose_with`) on `clock`,
/// its bodies in the filesystem blob store at `blobs`, ticking only when
/// settled.
pub fn live_config(clock: ManualClock, blobs: &Path) -> LiveConfig {
    LiveConfig {
        surface: options::in_process(clock.clone()).expect("surface options"),
        clock: LiveClock::Manual(clock),
        blobs: BlobConfig::Fs {
            root: blobs.to_path_buf(),
        },
        bus: BusConfig::default(),
        pipeline: Settings::default(),
        flow: options::flow().expect("flow options"),
        provenance: ProvenanceConfig::default(),
        extract: ExtractConfig::default(),
        threading: ThreadConfig::default(),
        ticking: Ticking::OnSettle,
        seed: SEED,
        capture: None,
        exchange_log: None,
    }
}

/// Where one run keeps what outlives its processes: the spool directory
/// and the blob store, both on the "volume" a restart reopens, and the
/// clock that only moves forward.
pub struct Volume {
    _dir: Option<tempfile::TempDir>,
    pub spool: PathBuf,
    pub blobs: PathBuf,
    pub clock: ManualClock,
}

impl Volume {
    pub fn new(start: Timestamp) -> Self {
        let dir = tempfile::tempdir().expect("a temp dir");
        Self {
            spool: dir.path().to_path_buf(),
            blobs: dir.path().join("blobs"),
            _dir: Some(dir),
            clock: ManualClock::at(start),
        }
    }

    /// A process's spool in `<spool>/spool`, reading the bodies another
    /// process (a gateway) stored at `blobs`.
    pub fn at(spool: PathBuf, blobs: PathBuf, clock: ManualClock) -> Self {
        Self {
            _dir: None,
            spool,
            blobs,
            clock,
        }
    }
}

/// `work` within [`WAIT`], or a panic with `live`'s diagnosis.
pub async fn within<T>(live: &Live<PgSet>, what: &str, work: impl Future<Output = T>) -> T {
    bounded(WAIT, what, work, || live.diagnose())
        .await
        .unwrap_or_else(|stalled| panic!("{stalled}"))
}

/// The diagnosis of a database without a live process (after a kill).
pub async fn diagnose_db(pool: &PgPool) -> String {
    let outbox: Result<(i64,), _> = sqlx::query_as(OUTBOX_ROWS).fetch_one(pool).await;
    let pending: Result<(i64,), _> = sqlx::query_as(PENDING_DELIVERIES).fetch_one(pool).await;
    format!("outbox rows: {outbox:?}; pending deliveries: {pending:?}")
}

/// A pool of its own on `db`, large enough for a pipeline.
pub fn pipeline_pool(db: &TestDb) -> PgPool {
    let settings = PoolSettings::new(
        NonZeroU32::MIN.saturating_add(11),
        0,
        Duration::from_secs(30),
    )
    .expect("pool settings");
    lazy_pool(&StoreConfig::new(db.url().clone(), settings))
}

/// Open the parts on `volume`, retrying while the spool directory's
/// `LOCK` is still held: a killed process's last spool handles drop with
/// its aborted tasks, as a crashed process's file locks go with it.
async fn open_parts(db: &TestDb, volume: &Volume, ids: PgIds) -> PgParts {
    let deadline = Instant::now() + WAIT;
    loop {
        match PgParts::open(
            pipeline_pool(db),
            bus_config(),
            spool_config(&volume.spool),
            secret(),
            ids,
        )
        .await
        {
            Ok(parts) => return parts,
            Err(PgPartsError::Spool(SpoolError::Locked { .. })) if Instant::now() < deadline => {
                tokio::time::sleep(POLL).await;
            }
            Err(error) => panic!("the postgres parts: {error}"),
        }
    }
}

/// A Postgres-mode live process over `db` and `volume` minting ids from
/// `ids`; its recovery bounded by [`WAIT`].
pub async fn start_pg(db: &TestDb, volume: &Volume, ids: PgIds) -> Live<PgSet> {
    let parts = open_parts(db, volume, ids).await;
    let probe = parts.clone();
    bounded(
        WAIT,
        "the postgres live process's recovery",
        Live::<PgSet>::start_pg(live_config(volume.clock.clone(), &volume.blobs), parts),
        || async move { probe.diagnose(NonZeroU16::MIN).await },
    )
    .await
    .unwrap_or_else(|stalled| panic!("{stalled}"))
    .expect("the postgres live process starts")
}

/// The `generation`-th process of a run: seeded from `SEED + generation`.
pub async fn start_generation(db: &TestDb, volume: &Volume, generation: u64) -> Live<PgSet> {
    start_pg(db, volume, PgIds::Seeded(SEED.wrapping_add(generation))).await
}

/// Stop gracefully: drain, checkpoint, ack, stop; then close the spool.
pub async fn stop_pg(live: Live<PgSet>) {
    let spool = live.layers().spool.clone();
    let diagnosis = live.diagnose().await;
    if tokio::time::timeout(WAIT, live.shutdown(Instant::now() + DRAIN))
        .await
        .is_err()
    {
        panic!("the shutdown did not finish within {WAIT:?}; before it:\n{diagnosis}");
    }
    if tokio::time::timeout(WAIT, spool.close()).await.is_err() {
        panic!("closing the spool did not finish within {WAIT:?}");
    }
}

/// Kill as a crash would (`Live::kill`: every task aborted, nothing
/// drained, acked or checkpointed) and stop the spool's drainer; the
/// spool's files stay as they were. Returns what the database held right
/// after, for the test's log.
pub async fn kill_pg(live: Live<PgSet>, db: &TestDb) -> String {
    let spool = live.layers().spool.clone();
    let pool = live.layers().pool.clone();
    let probe = live.layers().pool.clone();
    let diagnosis = bounded(WAIT, "the kill", live.kill(), || async move {
        no_live_diagnosis(&probe).await
    })
    .await;
    if let Err(stalled) = diagnosis {
        panic!("{stalled}");
    }
    if tokio::time::timeout(WAIT, spool.close()).await.is_err() {
        panic!("stopping the killed process's spool drainer did not finish within {WAIT:?}");
    }
    drop(spool);
    drop(pool);
    diagnose_db(db.pool()).await
}

/// A diagnosis for a wait with no live process to ask: the pool's.
async fn no_live_diagnosis(pool: &PgPool) -> PgDiagnosis {
    let bus = crosstalk_transport::PgBus::new(
        pool.clone(),
        crosstalk_gateway::live::pg::bus_clock(),
        bus_config(),
    )
    .expect("a bus to read the groups with");
    let groups: Vec<_> = [
        crosstalk_gateway::live::Slot::L3Reconstruct,
        crosstalk_gateway::live::Slot::L4Provenance,
        crosstalk_gateway::live::Slot::L5Flow,
        crosstalk_gateway::live::Slot::L6Classify,
        crosstalk_gateway::live::Slot::L7Topology,
    ]
    .iter()
    .map(|slot| slot.group())
    .collect();
    let diagnosis = crosstalk_gateway::live::pg::diagnose::diagnose(
        crosstalk_gateway::live::pg::diagnose::DiagnoseFrom {
            pool,
            bus: &bus,
            spool: None,
            groups: &groups,
            shards: NonZeroU16::MIN,
            status: None,
            watermark: None,
        },
    )
    .await;
    bus.shutdown();
    diagnosis
}

/// The trusted operator's caller on `live`.
pub async fn caller(live: &Live<PgSet>) -> Caller {
    within(live, "a caller", live.caller(RequestIdentity::Anonymous))
        .await
        .expect("a caller")
}

/// Feed `scenario` through `live`'s pipeline, moving the volume's clock.
pub async fn feed(live: &Live<PgSet>, volume: &Volume, scenario: &Scenario) {
    let clock = volume.clock.clone();
    within(
        live,
        "feeding the scenario",
        crosstalk_e2e::feed(scenario, live.pipeline(), |at| clock.set(at)),
    )
    .await
    .expect("fed");
}

/// Settle `live` to `until`.
pub async fn settle(live: &Live<PgSet>, until: Timestamp) {
    within(
        live,
        &format!("the settle to {}", until.as_micros()),
        live.settle(until),
    )
    .await
    .expect("settles");
}

/// `live`'s answer set now.
pub async fn read(live: &Live<PgSet>, scenario: &Scenario) -> Answers {
    let caller = caller(live).await;
    within(
        live,
        "the reads",
        answers(
            live.surface().as_ref(),
            &caller,
            &live.stores().transmissions,
            scenario,
            live.watermark(),
        ),
    )
    .await
    .expect("answers")
}

/// The scenario's end, and an hour past it.
pub fn passes(scenario: &Scenario) -> [Timestamp; 2] {
    let ends = scenario.ends_at();
    [ends, Timestamp::from_micros(ends.as_micros() + AFTER)]
}

/// The uninterrupted run's answers at each of [`passes`]: the memory
/// composition fed the whole scenario and settled (equal to an
/// uninterrupted Postgres-mode run's, `postgres::postgres_mode_answers_as_memory_mode_and_after_a_restart`).
pub async fn uninterrupted(scenario: &Scenario) -> [Answers; 2] {
    let memory = compose_with(scenario.start, Ticking::OnSettle)
        .await
        .expect("the memory composition");
    let clock = memory.clock.clone();
    crosstalk_e2e::feed(scenario, &memory.pipeline, |at| clock.set(at))
        .await
        .expect("fed");
    let mut out = Vec::new();
    for until in passes(scenario) {
        memory.live().settle(until).await.expect("settles");
        out.push(
            answers(
                memory.surface.as_ref(),
                &memory.caller,
                &memory.stores.transmissions,
                scenario,
                memory.live().watermark(),
            )
            .await
            .expect("memory answers"),
        );
    }
    memory.shutdown().await;
    let [ends, after]: [Answers; 2] = out.try_into().expect("two passes");
    assert!(
        after.detected(),
        "the relay is detected: {}",
        after.transmissions
    );
    [ends, after]
}

/// The scenario's exchanges in `range` only (`feed` takes a scenario).
pub fn part(scenario: &Scenario, range: std::ops::Range<usize>) -> Scenario {
    Scenario {
        exchanges: scenario.exchanges[range].to_vec(),
        ..scenario.clone()
    }
}

/// Assert `observed` equals `expected` once minted ids are renamed.
pub fn assert_same(observed: &Answers, expected: &Answers, what: &str) {
    assert_eq!(observed.renamed(), expected.renamed(), "{what}");
}

/// Settle the restarted `live` through both passes and compare each with
/// the uninterrupted run's.
pub async fn finish_and_compare(live: &Live<PgSet>, scenario: &Scenario, expected: &[Answers; 2]) {
    for (pass, until) in passes(scenario).into_iter().enumerate() {
        settle(live, until).await;
        let observed = read(live, scenario).await;
        assert_same(
            &observed,
            &expected[pass],
            &format!("pass {pass}: the restarted run differs from the uninterrupted one"),
        );
    }
}

/// How much stack the gateway tests give every thread: debug builds of the
/// pipeline's futures run deep, and an overflow aborts the whole binary.
pub const BIG_STACK: usize = 16 << 20;

/// Run `test` on a thread with [`BIG_STACK`], on a multi-thread runtime
/// whose workers (`gateway-test-worker`) have it too.
pub fn big_stack<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    let thread = std::thread::Builder::new()
        .name("gateway-test".to_owned())
        .stack_size(BIG_STACK)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(BIG_STACK)
                .thread_name("gateway-test-worker")
                .enable_all()
                .build()
                .expect("a runtime");
            runtime.block_on(test());
        })
        .expect("the test thread starts");
    if let Err(panic) = thread.join() {
        std::panic::resume_unwind(panic);
    }
}
