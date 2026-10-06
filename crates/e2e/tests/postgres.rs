//! Postgres mode against memory mode: the wiki relay fed through
//! `Live` over the memory stores and through `Live::start_pg` over a fresh
//! Postgres database, both settled on the same clock, read back through
//! the same `QueryApi` calls. The answers are equal: agents (the same
//! seed mints the same ids), the topology graph, the edge's transmissions,
//! the transmission rows and evidence pages, the channels, every stored
//! transmission and the watermark. Then the Postgres process restarts over
//! the same database and answers the same again: everything committed
//! before the stop is visible after it.
//!
//! Needs the test database (`TEST_DATABASE_URL`, `TestDb::new_or_skip`);
//! skips, passing, without one.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::PgIds;
use crosstalk_e2e::read::{self, ReadError};
use crosstalk_e2e::scenario::{DEFAULT_START, Scenario};
use crosstalk_e2e::{compose_with, feed, options};
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_gateway::live::pg::{PgParts, PgSet};
use crosstalk_gateway::live::{BlobConfig, Live, LiveClock, LiveConfig, Ticking};
use crosstalk_gateway::pipeline::Settings;
use crosstalk_gateway::store::lazy_pool;
use crosstalk_gateway::store::migrations::migrate_all;
use crosstalk_memory::support::ManualClock;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_reconstruct::thread::ThreadConfig;
use crosstalk_spec::ids::{DeploymentSecret, KeyedHasher, SecretVersion};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l8_surface::operators::RequestIdentity;
use crosstalk_spec::interfaces::l8_surface::{Caller, QueryApi};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::{PoolSettings, StoreConfig, TestDb};
use crosstalk_transport::{BusConfig, NonZeroDuration, PgBusConfig, SpoolConfig};
use tokio::time::Instant;

const SECRET_HEX: &str = "5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2";
const SEED: u64 = 0xE2E;
const DRAIN: Duration = Duration::from_secs(10);

/// One hour past the scenario's end: every window closed, every
/// suspicion expired or confirmed.
const AFTER: u64 = 3_600_000_000;

fn secret() -> Arc<KeyedHasher> {
    let secret =
        DeploymentSecret::from_hex(SecretVersion(1), SECRET_HEX).expect("a valid test secret");
    Arc::new(KeyedHasher::new(secret))
}

fn bus_config() -> PgBusConfig {
    PgBusConfig {
        poll: NonZeroDuration::new(Duration::from_millis(20)).expect("non-zero"),
        ..PgBusConfig::default()
    }
}

fn spool_config(dir: &Path) -> SpoolConfig {
    SpoolConfig::with_limits(
        dir.join("spool"),
        NonZeroU64::new(1 << 26).expect("non-zero"),
        NonZeroU64::new(1 << 22).expect("non-zero"),
        NonZeroUsize::new(64).expect("non-zero"),
        NonZeroDuration::new(Duration::from_millis(10)).expect("non-zero"),
    )
    .expect("spool bounds")
}

/// The composition's config (`crosstalk_e2e::compose_with`), on `clock`.
fn live_config(clock: ManualClock) -> LiveConfig {
    LiveConfig {
        surface: options::in_process(clock.clone()).expect("surface options"),
        clock: LiveClock::Manual(clock),
        blobs: BlobConfig::Memory,
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

/// Everything the comparison reads, as debug text per answer.
#[derive(Debug, PartialEq, Eq)]
struct Answers {
    agents: String,
    edges: String,
    edge_transmissions: String,
    summaries: String,
    evidence: String,
    channels: String,
    transmissions: String,
    watermark: u64,
}

async fn answers<Q, T>(
    surface: &Q,
    caller: &Caller,
    transmissions: &T,
    scenario: &Scenario,
    watermark: Timestamp,
) -> Result<Answers, ReadError>
where
    Q: QueryApi + Sync,
    T: TransmissionStore + Sync,
{
    let window = read::window(scenario, Duration::from_secs(300))?;
    let agents = read::agents(surface, caller, scenario, window).await?;
    let edges = read::edges(surface, caller, window).await?;
    let edge_transmissions = match agents
        .agents
        .and_then(|agents| read::channel_edge(&edges, agents))
    {
        Some(edge) => format!(
            "{:?}",
            read::edge_transmissions(surface, caller, edge, window).await?
        ),
        None => "no channel edge".to_owned(),
    };
    let stored = read::all_transmissions(transmissions).await?;
    let ids: Vec<_> = stored.iter().map(|transmission| transmission.id).collect();
    let summaries = match ids.is_empty() {
        true => "none".to_owned(),
        false => format!("{:?}", read::summaries(surface, caller, ids.clone()).await?),
    };
    let mut evidence = Vec::new();
    for id in &ids {
        evidence.push(format!("{:?}", read::evidence(surface, caller, *id).await?));
    }
    Ok(Answers {
        agents: format!("{agents:?}"),
        edges: format!("{edges:?}"),
        edge_transmissions,
        summaries,
        evidence: evidence.join("\n"),
        channels: format!("{:?}", read::channels(surface, caller).await?),
        transmissions: format!("{stored:?}"),
        watermark: watermark.as_micros(),
    })
}

/// A Postgres-mode live process over `db`, its spool in `dir`.
async fn start_pg(db: &TestDb, dir: &Path, clock: ManualClock) -> Live<PgSet> {
    let settings = PoolSettings::new(
        NonZeroU32::MIN.saturating_add(11),
        0,
        Duration::from_secs(30),
    )
    .expect("pool settings");
    let pool = lazy_pool(&StoreConfig::new(db.url().clone(), settings));
    let parts = PgParts::open(
        pool,
        Arc::new(clock.clone()),
        bus_config(),
        spool_config(dir),
        secret(),
        PgIds::Seeded(SEED),
    )
    .await
    .expect("the postgres parts");
    Live::<PgSet>::start_pg(live_config(clock), parts)
        .await
        .expect("the postgres live process starts")
}

async fn stop_pg(live: Live<PgSet>) {
    let spool = live.layers().spool.clone();
    live.shutdown(Instant::now() + DRAIN).await;
    spool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_mode_answers_as_memory_mode_and_after_a_restart() {
    let Some(db) = TestDb::new_or_skip("postgres_mode_answers_as_memory_mode_and_after_a_restart")
        .await
        .expect("the test database")
    else {
        return;
    };
    migrate_all(&db.store()).await.expect("every layer migrates");
    let scenario = Scenario::wiki_relay(DEFAULT_START);
    let ends = scenario.ends_at();
    let after = Timestamp::from_micros(ends.as_micros() + AFTER);

    // Memory mode.
    let memory = compose_with(scenario.start, Ticking::OnSettle)
        .await
        .expect("the memory composition");
    let clock = memory.clock.clone();
    feed(&scenario, &memory.pipeline, |at| clock.set(at))
        .await
        .expect("fed");
    let mut expected = Vec::new();
    for until in [ends, after] {
        memory.live().settle(until).await.expect("settles");
        expected.push(
            answers(
                memory.surface.as_ref(),
                &memory.caller,
                &memory.stores.transmissions,
                &scenario,
                memory.live().watermark(),
            )
            .await
            .expect("memory answers"),
        );
    }
    memory.shutdown().await;
    assert!(
        expected[1].transmissions.contains("Channel"),
        "the relay is detected in memory mode: {}",
        expected[1].transmissions
    );

    // Postgres mode, same traffic, same clock, same seed.
    let dir = tempfile::tempdir().expect("a temp dir");
    let clock = ManualClock::at(scenario.start);
    let live = start_pg(&db, dir.path(), clock.clone()).await;
    let caller = live
        .caller(RequestIdentity::Anonymous)
        .await
        .expect("a caller");
    feed(&scenario, live.pipeline(), |at| clock.set(at))
        .await
        .expect("fed");
    for (pass, until) in [ends, after].into_iter().enumerate() {
        live.settle(until).await.expect("settles");
        let observed = answers(
            live.surface().as_ref(),
            &caller,
            &live.stores().transmissions,
            &scenario,
            live.watermark(),
        )
        .await
        .expect("postgres answers");
        assert_eq!(observed, expected[pass], "pass {pass}: postgres mode differs");
    }
    stop_pg(live).await;

    // Restarted over the same database: the same answers before anything
    // new happens. (A spool directory of its own: the stopped process's
    // spool, drained and closed, may still be releasing its `LOCK` as its
    // aborted tasks drop; a real restart is a new process.)
    let restarted = start_pg(&db, &dir.path().join("restarted"), clock.clone()).await;
    let caller = restarted
        .caller(RequestIdentity::Anonymous)
        .await
        .expect("a caller");
    restarted.settle(after).await.expect("settles");
    let observed = answers(
        restarted.surface().as_ref(),
        &caller,
        &restarted.stores().transmissions,
        &scenario,
        restarted.watermark(),
    )
    .await
    .expect("answers after the restart");
    assert_eq!(observed, expected[1], "the restarted process differs");
    stop_pg(restarted).await;
    db.close().await.expect("drops");
}
