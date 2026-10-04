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

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::http::UrlError;
use agent::{Agent, Shared};
use config::SwarmConfig;
use stats::{CollectorSetup, Report, collect};

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
    tracing::info!(
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
        "swarm starting"
    );
    let shared = Arc::new(Shared {
        gateway: config.gateway.client(gateway_addr, config.idle_timeout),
        wiki: config.wiki.client(wiki_addr, config.idle_timeout),
        config: config.clone(),
    });
    let (events, inbox) = mpsc::channel(4096);
    let collector = tokio::spawn(collect(
        CollectorSetup {
            agents,
            keys,
            seed: config.seed,
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
