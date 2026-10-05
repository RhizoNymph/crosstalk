//! The swarm driver: N simulated agents talking to the model through the
//! crosstalk proxy and to each other through the wiki.
//!
//! ```text
//! run ──spawn──▶ agent tasks (ramped) ──Event──▶ collector task ──▶ Report
//!   │ after `duration` (or SIGINT/SIGTERM): stop watch = true
//!   │ agents finish their current request, up to `grace`, then are aborted
//!   └ when every agent is gone the event channel closes and the collector returns
//! ```
//!
//! Agents share nothing mutable: the configuration and the two HTTP
//! clients are read-only behind an `Arc`, the wiki is the only shared
//! state (as it is for real agents), and figures go to the collector over
//! a channel.

pub mod agent;
pub mod config;
pub mod conversation;
pub mod stats;
pub mod tools;
pub mod truth;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::ids::{SeededRandom, UlidExhausted, UlidGenerator};
use crosstalk_spec::support::{Clock, SystemClock, Timestamp};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::http::UrlError;
use crate::knobs::Rng;
use agent::{Agent, Shared, agent_name};
use config::SwarmConfig;
use stats::{CollectorSetup, Report, collect};
use truth::RunInfo;

/// The run's time: one wall-clock reading at the start plus monotonic
/// elapsed time, so every stamp of the run is ordered like the events and
/// `unix_ms == started_at_unix_ms + at_ms` holds exactly.
#[derive(Debug, Clone, Copy)]
pub struct RunClock {
    start: Instant,
    start_unix_ms: u64,
}

/// A moment of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    /// Since the run started.
    pub at_ms: u64,
    pub unix_ms: u64,
}

impl RunClock {
    /// Starts now, reading the wall clock once from `clock`.
    pub fn start(clock: &dyn Clock) -> Self {
        Self {
            start: Instant::now(),
            start_unix_ms: clock.now().as_micros() / 1000,
        }
    }

    pub fn started_at_unix_ms(&self) -> u64 {
        self.start_unix_ms
    }

    pub fn now(&self) -> Stamp {
        let at_ms = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        Stamp {
            at_ms,
            unix_ms: self.start_unix_ms.saturating_add(at_ms),
        }
    }
}

/// Crockford's base32 alphabet, as ULIDs are written.
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// A ULID as its 26 characters.
pub fn ulid_text(raw: u128) -> String {
    (0..26u32)
        .rev()
        .map(|digit| char::from(CROCKFORD[((raw >> (5 * digit)) & 0x1f) as usize]))
        .collect()
}

/// The run's id: a ULID from the spec's generator, stamped with the run's
/// start and with its random part drawn from the seed and that start. The
/// run's other choices are all seeded, so the id stays a function of what
/// the header records (`seed`, `started_at_unix_ms`), while two runs with
/// the same seed differ by their start time and two runs started in the
/// same millisecond differ by their seed.
pub fn run_id(seed: u64, started_at_unix_ms: u64) -> Result<String, UlidExhausted> {
    let random = Rng::derive(seed, &[b"run", &started_at_unix_ms.to_le_bytes()]).next_u64();
    let mut ids = UlidGenerator::new(Arc::new(SystemClock), SeededRandom::new(random));
    let raw = ids.next_at(Timestamp::from_micros(
        started_at_unix_ms.saturating_mul(1000),
    ))?;
    Ok(ulid_text(raw))
}

/// How long to wait for the gateway's and the wiki's names to resolve.
const RESOLVE_PATIENCE: Duration = Duration::from_secs(60);

/// Why the swarm could not run.
#[derive(Debug, thiserror::Error)]
pub enum SwarmError {
    #[error("gateway: {0}")]
    Gateway(UrlError),
    #[error("wiki: {0}")]
    Wiki(UrlError),
    #[error("the collector task failed: {0}")]
    Collector(#[from] tokio::task::JoinError),
    #[error("minting the run id: {0}")]
    RunId(UlidExhausted),
}

/// Runs the swarm for `config.duration` (or until `shutdown`) and returns
/// the report.
pub async fn run(
    config: SwarmConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<Report, SwarmError> {
    let gateway_addr = config
        .gateway
        .resolve_patiently(RESOLVE_PATIENCE)
        .await
        .map_err(SwarmError::Gateway)?;
    let wiki_addr = config
        .wiki
        .resolve_patiently(RESOLVE_PATIENCE)
        .await
        .map_err(SwarmError::Wiki)?;
    let agents = config.agents.get();
    let keys = agents.div_ceil(config.agents_per_key.get());
    let clock = RunClock::start(&SystemClock);
    let run = run_id(config.seed, clock.started_at_unix_ms()).map_err(SwarmError::RunId)?;
    tracing::info!(
        run = %run,
        gateway = %config.gateway,
        gateway_addr = %gateway_addr,
        wiki = %config.wiki,
        agents,
        keys,
        duration_s = config.duration.as_secs(),
        think_ms = %config.think_ms,
        turns = %config.turns.get(),
        write_fraction = config.mix.write().get(),
        read_fraction = config.mix.read().get(),
        pages = config.pages.get(),
        topics = config.topics.get(),
        stream_fraction = config.stream_fraction.get(),
        claude_code_shape = config.claude_code_shape,
        seed = config.seed,
        scenario = %config.scenario,
        "swarm starting"
    );
    let shared = Arc::new(Shared {
        gateway: config.gateway.client(gateway_addr, config.idle_timeout),
        wiki: config.wiki.client(wiki_addr, config.idle_timeout),
        config: config.clone(),
        clock,
    });
    let (events, inbox) = mpsc::channel(4096);
    let collector = tokio::spawn(collect(
        CollectorSetup {
            info: RunInfo {
                run,
                seed: config.seed,
                scenario: config.scenario,
                agents,
                keys,
                agents_per_key: config.agents_per_key.get(),
                claude_code_shape: config.claude_code_shape,
                started_at_unix_ms: clock.started_at_unix_ms(),
                gateway_url: config.gateway.url(),
                wiki_url: config.wiki.url(),
            },
            agent_names: (0..agents).map(agent_name).collect(),
            ground_truth: config.ground_truth.clone(),
            progress_every: Duration::from_secs(10),
        },
        inbox,
    ));
    let (stop, stopped) = watch::channel(false);
    let mut tasks = JoinSet::new();
    for index in 0..agents {
        let agent = Agent::new(&config, index);
        let delay = config.ramp.mul_f64(f64::from(index) / f64::from(agents));
        let (shared, events, mut stopped) = (Arc::clone(&shared), events.clone(), stopped.clone());
        tasks.spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = stopped.changed() => return,
            }
            agent::run(agent, shared, events, stopped).await;
        });
    }
    drop(events);
    tokio::pin!(shutdown);
    tokio::select! {
        () = tokio::time::sleep(config.duration) => tracing::info!("run time over; stopping agents"),
        () = &mut shutdown => tracing::info!("interrupted; stopping agents"),
    }
    // Every receiver is in a task we still hold, so the send cannot fail
    // unless every agent already ended, which needs no signal.
    let _ = stop.send(true);
    let drained = tokio::time::timeout(config.grace, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            still_running = tasks.len(),
            grace_s = config.grace.as_secs(),
            "agents still in flight after the grace period; aborting them"
        );
        tasks.shutdown().await;
    }
    Ok(collector.await?)
}
