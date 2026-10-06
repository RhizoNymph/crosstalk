//! The gateway in Postgres mode: a `store` section configured.
//!
//! ```text
//! start (no database needed):
//!   lazy pool ─▶ PgBus::new ─▶ SpoolingBus::open(<data dir>/spool, Gated<PgBus>)   (LOCK, torn tail)
//!   capture pipeline over the spool ─▶ proxy forwards and captures at once
//!   listeners: proxy, api (503 until the surface exists), ops
//! pipeline task (roles all, proxy, pipeline):
//!   wait for the database ─▶ migrations at head? (behind: not ready, re-checked)
//!   ─▶ pipeline lock (held elsewhere: not ready, retried)
//!   ─▶ Live::start_pg (recovery; the gate opens; the spool drains)
//!   ─▶ the API router is set ─▶ running until shutdown, or until the lock is lost
//! api task (role api):
//!   wait for the database ─▶ migrations at head ─▶ PgStores over PgBus, the surface
//!   hosted (node facts rebuilt every few seconds) ─▶ the API router is set
//! ```
//!
//! Forwarding never waits on any of it (`ingress.proxy.forwarding-independent-of-capture-store`):
//! the proxy hands exchanges to the capture stage over a bounded channel,
//! and capture publishes into the spool, which takes them while the
//! database is down and refuses (counted `spool_full`) only once full.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crosstalk_api::http::{Auth, HttpApi, HttpConfig, StaticTokens};
use crosstalk_api::pg::{PgSettings, PgStores};
use crosstalk_api::{CursorSecret, InProcess, InProcessOptions, PgIds, PgOpen};
use crosstalk_spec::ids::{KeyedHasher, SeededRandom};
use crosstalk_spec::support::Clock;
use crosstalk_store::sqlx::PgPool;
use crosstalk_store::{DatabaseUrl, SerializableRetry};
use crosstalk_transport::blob::FsBlobStore;
use crosstalk_transport::{PgBus, SpoolingBus};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::late::LateRouter;
use super::{StartError, access};
use crate::config::GatewayConfig;
use crate::live::pg::PgSet;
use crate::live::recovery::{
    LockState, MigrationState, PipelinePhase, PipelineStatus, StatusReporter,
};
use crate::live::{
    BlobConfig, Live, LiveBlobs, LiveClock, LiveConfig, LiveDrained, LiveError, LiveReporter,
    StagesRunning, Ticking,
};
use crate::log::ExchangeLog;
use crate::pipeline::{Bodies, Deps, Pipeline, Settings};
use crate::role::Role;
use crate::spool::{Gate, Gated, LiveBus};
use crate::store::lock::PipelineLock;
use crate::store::migrations;
use crate::tasks::Tasks;

/// Where `serve`'s Postgres-mode id generators draw from: OS entropy, so
/// an id minted after a restart never repeats a persisted one
/// (`surface.ids.unique-across-restart`). Tests pass a fixed seed to
/// `Live::start_pg` instead.
pub const SERVE_IDS: PgIds = PgIds::Entropy;

/// How long the tasks wait between two looks at the database.
const RETRY: Duration = Duration::from_secs(2);

/// How long a reachability probe waits for a pooled connection.
const PROBE: Duration = Duration::from_secs(2);

/// How often the `api` role rebuilds its node facts from the stores.
const NODES_EVERY: Duration = Duration::from_secs(5);

/// What the ops listener and the API read once the live process exists.
#[derive(Debug, Clone, Default)]
pub struct Late {
    pub router: LateRouter,
    pub reporter: Arc<OnceLock<LiveReporter>>,
    pub stages: Arc<OnceLock<StagesRunning>>,
    pub log_tasks: Arc<OnceLock<Tasks>>,
}

/// The API binding the surface is served on, once it exists.
#[derive(Clone)]
pub struct ApiBinding {
    pub tokens: StaticTokens,
    pub clock: Arc<dyn Clock>,
}

/// What the pipeline task hands back when told to stop: the running live
/// process and the lock it holds, `None` when it never started (or lost
/// the lock and stopped itself).
pub type PipelineHeld = Option<(Live<PgSet>, PipelineLock)>;

/// The process's Postgres side.
pub struct PgRunning {
    pub pool: PgPool,
    pub bus: PgBus,
    pub spool: Option<LiveBus>,
    pub capture: Option<JoinHandle<()>>,
    pub capture_pipeline: Option<Arc<Pipeline<LiveBlobs, LiveBus>>>,
    pub pipeline: Option<StopHandle<PipelineHeld>>,
    pub api: Option<StopHandle<()>>,
    pub status: StatusReporter,
    pub late: Late,
}

/// A task that ends when told to, returning what it holds.
pub struct StopHandle<T> {
    stop: oneshot::Sender<()>,
    task: JoinHandle<T>,
}

impl<T> StopHandle<T> {
    /// Tell the task to stop and wait for what it returns (`None` when it
    /// failed).
    pub async fn stop(self) -> Option<T> {
        let _ = self.stop.send(());
        match self.task.await {
            Ok(value) => Some(value),
            Err(error) => {
                tracing::error!(error = %error, "a postgres task failed");
                None
            }
        }
    }
}

impl PgRunning {
    /// Stop in dependency order: capture drains its channel (the proxy is
    /// gone), the live process drains and stops, the API surface stops,
    /// the spool's drainer stops (its files stay), the bus and the pool
    /// close.
    pub async fn shutdown(self, deadline: Instant) -> LiveDrained {
        let capture = match self.capture {
            Some(task) => {
                let mut task = task;
                match tokio::time::timeout_at(deadline, &mut task).await {
                    Ok(_) => true,
                    Err(_) => {
                        tracing::warn!("the capture stage did not drain before the deadline");
                        task.abort();
                        false
                    }
                }
            }
            None => true,
        };
        let mut drained = LiveDrained {
            capture,
            stages: true,
            log: true,
        };
        if let Some(pipeline) = self.pipeline
            && let Some(Some((live, lock))) = pipeline.stop().await
        {
            let live_drained = live.shutdown(deadline).await;
            drained.stages = live_drained.stages;
            drained.log = live_drained.log;
            drop(lock);
        }
        if let Some(api) = self.api {
            api.stop().await;
        }
        if let Some(spool) = &self.spool {
            spool.close().await;
        }
        self.bus.shutdown();
        self.pool.close().await;
        drained
    }
}

/// Build the bus, the spool and the capture pipeline: no database needed.
pub struct CaptureSide {
    pub pool: PgPool,
    pub bus: PgBus,
    pub gate: Gate,
    pub spool: Option<LiveBus>,
    pub pipeline: Option<Arc<Pipeline<LiveBlobs, LiveBus>>>,
}

/// The bus over the lazy pool; for a pipeline role, the spool in front of
/// it and the capture pipeline over the spool.
pub async fn capture_side(
    config: &GatewayConfig,
    role: Role,
    pool: PgPool,
    blobs: &FsBlobStore,
    clock: &Arc<dyn Clock>,
) -> Result<CaptureSide, StartError> {
    let section = config.store.ok_or(StartError::NoStore)?;
    let bus = PgBus::new(pool.clone(), Arc::clone(clock), section.bus).map_err(StartError::Bus)?;
    let gate = Gate::closed();
    if role == Role::Api {
        return Ok(CaptureSide {
            pool,
            bus,
            gate,
            spool: None,
            pipeline: None,
        });
    }
    let spool_config = config.spool.spool_config(config.data_dir()?)?;
    let spool = SpoolingBus::open(Gated::new(bus.clone(), gate.clone()), spool_config)
        .await
        .map_err(StartError::Spool)?;
    let pipeline = Pipeline::build(
        Settings::from_config(config),
        Deps {
            // `FsBlobStore` never drops a body: the spec's `BlobStore` has
            // no delete, and nothing prunes `blobs.root`.
            bodies: Bodies::SkipStored,
            ..Deps::stores(
                LiveBlobs::Fs(blobs.clone()),
                spool.clone(),
                SeededRandom::from_entropy(),
            )
        },
        Arc::clone(clock),
    )
    .await?;
    Ok(CaptureSide {
        pool,
        bus,
        gate,
        spool: Some(spool),
        pipeline: Some(Arc::new(pipeline)),
    })
}

/// What the pipeline task starts the live process from.
pub struct PipelineStart {
    pub config: GatewayConfig,
    pub role: Role,
    pub clock: LiveClock,
    pub blobs: FsBlobStore,
    pub pool: PgPool,
    pub url: DatabaseUrl,
    pub bus: PgBus,
    pub spool: LiveBus,
    pub gate: Gate,
    pub pipeline: Arc<Pipeline<LiveBlobs, LiveBus>>,
    pub secret: Arc<KeyedHasher>,
    pub status: StatusReporter,
    pub late: Late,
    pub api: Option<ApiBinding>,
}

/// Run the pipeline task: see the module docs.
pub fn spawn_pipeline(start: PipelineStart) -> StopHandle<PipelineHeld> {
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(run_pipeline(start, stopped));
    StopHandle { stop, task }
}

/// Wait `RETRY`, or return `true` when told to stop.
async fn pause(stop: &mut oneshot::Receiver<()>) -> bool {
    tokio::select! {
        _ = stop => true,
        () = tokio::time::sleep(RETRY) => false,
    }
}

/// Wait until a pooled connection answers; `false` when told to stop.
async fn wait_for_database(
    pool: &PgPool,
    status: &StatusReporter,
    stop: &mut oneshot::Receiver<()>,
) -> bool {
    loop {
        match tokio::time::timeout(PROBE, pool.acquire()).await {
            Ok(Ok(_connection)) => return true,
            Ok(Err(error)) => {
                tracing::debug!(error = %crosstalk_store::classify(&error), "postgres not reachable yet");
            }
            Err(_) => tracing::debug!("postgres did not answer in time"),
        }
        status.phase(PipelinePhase::WaitingForDatabase);
        if pause(stop).await {
            return false;
        }
    }
}

/// Wait until every layer's migrations are at head; `false` when told to
/// stop. `serve` never migrates.
async fn wait_for_migrations(
    pool: &PgPool,
    status: &StatusReporter,
    stop: &mut oneshot::Receiver<()>,
) -> bool {
    loop {
        match migrations::check_heads(pool).await {
            Ok(behind) if behind.is_empty() => {
                status.update(|status| status.migrations = MigrationState::AtHead);
                return true;
            }
            Ok(behind) => {
                let text = migrations::describe(&behind);
                tracing::error!(behind = %text, "migrations are behind; run `crosstalk migrate`");
                status.update(|status| {
                    status.migrations = MigrationState::Behind(text);
                    status.phase = PipelinePhase::WaitingForMigrations;
                });
            }
            Err(error) => {
                tracing::warn!(error = %error, "reading the applied migrations failed");
                status.phase(PipelinePhase::WaitingForDatabase);
            }
        }
        if pause(stop).await {
            return false;
        }
    }
}

/// The live process's config in Postgres mode.
async fn live_config(start: &PipelineStart) -> Result<LiveConfig, StartError> {
    let config = &start.config;
    let mut live = LiveConfig::new(start.clock.clone(), config.flow, 0)?;
    live.blobs = BlobConfig::Open(LiveBlobs::Fs(start.blobs.clone()));
    live.pipeline = Settings::from_config(config);
    live.extract = config.extract.clone();
    live.ticking = Ticking::Periodic;
    live.capture = None;
    live.exchange_log = match start.role.runs_pipeline() {
        true => Some(ExchangeLog::open(&config.exchange_log_path()?).await?),
        false => None,
    };
    live.surface.access = access(config.api.as_ref().map(|api| &api.operator));
    Ok(live)
}

async fn run_pipeline(start: PipelineStart, mut stop: oneshot::Receiver<()>) -> PipelineHeld {
    let status = start.status.clone();
    loop {
        if !wait_for_database(&start.pool, &status, &mut stop).await
            || !wait_for_migrations(&start.pool, &status, &mut stop).await
        {
            return None;
        }
        let lock = match PipelineLock::try_take(&start.url).await {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                tracing::warn!("the pipeline lock is held by another process; waiting");
                status.update(|status| {
                    status.lock = LockState::HeldElsewhere;
                    status.phase = PipelinePhase::WaitingForLock;
                });
                if pause(&mut stop).await {
                    return None;
                }
                continue;
            }
            Err(error) => {
                tracing::warn!(error = %error, "taking the pipeline lock failed");
                if pause(&mut stop).await {
                    return None;
                }
                continue;
            }
        };
        status.update(|status| status.lock = LockState::Held);
        let config = match live_config(&start).await {
            Ok(config) => config,
            Err(error) => {
                status.phase(PipelinePhase::Stopped(error.to_string()));
                tracing::error!(error = %error, "the live process's config failed");
                let _ = stop.await;
                return None;
            }
        };
        let started = Live::start_pg(
            config,
            crate::live::pg::PgParts {
                pool: start.pool.clone(),
                bus: start.bus.clone(),
                spool: start.spool.clone(),
                gate: start.gate.clone(),
                pipeline: Some(Arc::clone(&start.pipeline)),
                secret: Arc::clone(&start.secret),
                ids: SERVE_IDS,
                retry: SerializableRetry::default(),
                status: status.clone(),
            },
        )
        .await;
        let live = match started {
            Ok(live) => live,
            Err(error) => {
                tracing::error!(error = %error, "the live process did not start");
                status.phase(PipelinePhase::Stopped(error.to_string()));
                drop(lock);
                if permanent(&error) {
                    let _ = stop.await;
                    return None;
                }
                if pause(&mut stop).await {
                    return None;
                }
                continue;
            }
        };
        let _ = start.late.reporter.set(live.reporter());
        let _ = start.late.stages.set(live.stages_running());
        let _ = start.late.log_tasks.set(live.tasks().clone());
        if let Some(api) = &start.api {
            match live.stores().operators.directory().await {
                Ok(Some(directory)) => {
                    let auth = Auth::fixed(directory, api.tokens.clone());
                    let http = HttpConfig {
                        frame_retention: live.surface().config().frame_retention,
                        clock: Arc::clone(&api.clock),
                    };
                    start
                        .late
                        .router
                        .set(HttpApi::new(Arc::clone(live.surface()), auth, http).router());
                }
                Ok(None) => tracing::error!("no operator directory stored; the API stays unavailable"),
                Err(error) => {
                    tracing::error!(error = ?error, "reading the operator directory failed; the API stays unavailable");
                }
            }
        }
        let mut lock = lock;
        return tokio::select! {
            _ = &mut stop => Some((live, lock)),
            why = lock.lost() => {
                status.update(|status| {
                    status.lock = LockState::Lost;
                    status.phase = PipelinePhase::Stopped(format!("pipeline lock lost: {why}"));
                });
                live.shutdown(Instant::now() + Duration::from_secs(5)).await;
                let _ = stop.await;
                None
            }
        };
    }
}

/// Whether a start failure needs an operator rather than a retry.
fn permanent(error: &LiveError) -> bool {
    matches!(
        error,
        LiveError::Restore(crosstalk_flow::consumer::FlowRestoreError::IncompatibleSnapshot(_))
            | LiveError::Flow(_)
            | LiveError::AckTimeout { .. }
            | LiveError::UnackedAboveCapacity { .. }
            | LiveError::TooManyShards(_)
            | LiveError::CaptureOutsidePipeline
    )
}

/// What the `api` role's surface is built from.
pub struct ApiStart {
    pub config: GatewayConfig,
    pub clock: Arc<dyn Clock>,
    pub blobs: FsBlobStore,
    pub pool: PgPool,
    pub bus: PgBus,
    pub secret: Arc<KeyedHasher>,
    pub status: StatusReporter,
    pub late: Late,
    pub api: ApiBinding,
}

/// The `api` role: the surface over the stores, no pipeline.
pub fn spawn_api(start: ApiStart) -> StopHandle<()> {
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(run_api(start, stopped));
    StopHandle { stop, task }
}

async fn run_api(start: ApiStart, mut stop: oneshot::Receiver<()>) {
    let status = start.status.clone();
    let hosted = loop {
        if !wait_for_database(&start.pool, &status, &mut stop).await
            || !wait_for_migrations(&start.pool, &status, &mut stop).await
        {
            return;
        }
        match host_api(&start).await {
            Ok(hosted) => break hosted,
            Err(error) => {
                tracing::warn!(error = %error, "the API surface did not start; retrying");
                status.phase(PipelinePhase::Stopped(error.to_string()));
                if pause(&mut stop).await {
                    return;
                }
            }
        }
    };
    status.phase(PipelinePhase::Running);
    let mut ticker = tokio::time::interval(NODES_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = &mut stop => break,
            _ = ticker.tick() => {
                if let Err(error) = hosted.nodes.rebuild().await {
                    tracing::warn!(error = %error, "node facts not rebuilt; retried");
                }
            }
        }
    }
    hosted.shutdown().await;
}

type ApiSurface = InProcess<PgStores<PgBus, LiveBlobs>>;

async fn host_api(start: &ApiStart) -> Result<ApiSurface, StartError> {
    let mut live = LiveConfig::new(LiveClock::Read(Arc::clone(&start.clock)), start.config.flow, 0)?;
    live.surface.access = access(start.config.api.as_ref().map(|api| &api.operator));
    let flow = crosstalk_flow::consumer::Settings::try_from(start.config.flow)
        .map_err(LiveError::Flow)?;
    let options: InProcessOptions = InProcessOptions {
        timing: flow.timing,
        ..live.surface
    };
    let (stores, _topology_relay) = PgStores::open(PgOpen {
        pool: start.pool.clone(),
        bus: start.bus.clone(),
        dead_letters: start.bus.dead_letters(),
        blobs: LiveBlobs::Fs(start.blobs.clone()),
        clock: Arc::clone(&start.clock),
        secret: Arc::clone(&start.secret),
        ids: SERVE_IDS,
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
            configure_sinks: false,
            threading: live.threading,
            retry: SerializableRetry::default(),
        },
    })
    .await
    .map_err(|error| StartError::Live(LiveError::Stores(error)))?;
    // The API role publishes only what an operator action decides; the
    // pipeline process relays L7's outbox. Nothing feeds this surface's
    // relay: the node facts are rebuilt on a timer, and the live feed
    // carries only config loads (live events need the pipeline process).
    let (_events, relayed) = mpsc::unbounded_channel();
    let hosted = InProcess::host(
        stores,
        options,
        relayed,
        CursorSecret::Derived(&start.secret),
        "gateway access config",
    )
    .await
    .map_err(|error| StartError::Live(LiveError::Surface(error)))?;
    if let Err(error) = hosted.surface.recover_interrupted().await {
        tracing::warn!(error = ?error, "interrupted operator actions not recorded yet");
    }
    match hosted.stores.operators.directory().await {
        Ok(Some(directory)) => {
            let auth = Auth::fixed(directory, start.api.tokens.clone());
            let http = HttpConfig {
                frame_retention: hosted.surface.config().frame_retention,
                clock: Arc::clone(&start.api.clock),
            };
            start
                .late
                .router
                .set(HttpApi::new(Arc::clone(&hosted.surface), auth, http).router());
        }
        Ok(None) => tracing::error!("no operator directory stored; the API stays unavailable"),
        Err(error) => tracing::error!(error = ?error, "reading the operator directory failed"),
    }
    Ok(hosted)
}

/// The status a Postgres-mode process starts with.
pub fn initial_status() -> StatusReporter {
    StatusReporter::new(PipelineStatus::waiting())
}
