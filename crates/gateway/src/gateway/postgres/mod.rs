//! The gateway in Postgres mode: a `store` section configured.
//!
//! ```text
//! start (no database needed):
//!   lazy pools ─▶ PgBus::publisher (capture's own pool) ─▶ SpoolingBus::open(<data dir>/spool, Gated<PgBus>)
//!   capture pipeline over the spool ─▶ proxy forwards and captures at once
//!   listeners: proxy, api (503 until the surface exists), ops
//! pipeline task (roles all, proxy, pipeline), one loop for the process's life:
//!   waiting:    the database answers ─▶ migrations at head? (behind: not ready, re-checked)
//!               ─▶ pipeline lock (held elsewhere: retried) ─▶ the epoch bumped ─▶ fencing:
//!               until no connection of an earlier incarnation (any process) is left
//!   starting:   incarnation n under epoch e: a pool (each connection joins the fence and
//!               checks e) and a PgBus of its own ─▶ Live::start_pg (recovery; the gate
//!               opens; the spool drains) ─▶ the API router and the ops handles set
//!   running:    until shutdown, or until the lock is lost
//!   abandoning: lost: the handles cleared, the pool closed (the fence), every task aborted
//!               and joined, nothing drained ─▶ waiting
//! api task (role api):
//!   wait for the database ─▶ migrations at head ─▶ PgStores over PgBus, the surface
//!   hosted (node facts rebuilt every few seconds) ─▶ the API router is set
//! ```
//!
//! Forwarding never waits on any of it (`ingress.proxy.forwarding-independent-of-capture-store`):
//! the proxy hands exchanges to the capture stage over a bounded channel,
//! and capture publishes into the spool, which takes them while the
//! database is down and refuses (counted `spool_full`) only once full. A
//! lost pipeline lock stops neither: capture appends through a bus and a
//! pool of its own, which no incarnation shares
//! (`ingress.capture.never-stopped-by-lock-loss`).

mod api;
mod pipeline;

use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::PgIds;
use crosstalk_api::http::StaticTokens;
use crosstalk_spec::ids::{KeyedHasher, SeededRandom};
use crosstalk_spec::support::Clock;
use crosstalk_store::DatabaseUrl;
use crosstalk_store::sqlx::PgPool;
use crosstalk_store::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use crosstalk_transport::blob::FsBlobStore;
use crosstalk_transport::{PgBus, SpoolingBus};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use self::api::{ApiStart, spawn_api};
pub use self::pipeline::{application_name, spawn_pipeline};
use super::StartError;
use super::late::{LateRouter, LateValue};
use crate::config::GatewayConfig;
use crate::live::pg::PgSet;
use crate::live::recovery::{MigrationState, PipelinePhase, PipelineStatus, StatusReporter};
use crate::live::{Live, LiveBlobs, LiveClock, LiveDrained, LiveReporter, StagesRunning};
use crate::pipeline::{Bodies, Deps, Pipeline, Settings};
use crate::role::Role;
use crate::spool::{Gate, Gated, LiveBus};
use crate::store::fence::{self, Epoch};
use crate::store::lock::{DETECTION, PipelineLock, SERVER_REAP};
use crate::store::timeouts::PipelineTimeouts;
use crate::store::{migrations, session_setup};
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

/// How long the migrations' head check may take before the database
/// counts as unreachable (a query across a silent partition never
/// answers).
const HEAD_CHECK: Duration = Duration::from_secs(10);

/// How long an abandoned incarnation's tasks may take to end once the lock
/// is lost: everything is aborted, so this only bounds the joins.
pub const LOST_SHUTDOWN: Duration = Duration::from_secs(1);

/// How long any close of a pool may wait for its connections to come
/// back. sqlx's `Pool::close` waits for every checked-out connection, and
/// one returned after a cancelled query pings without a deadline: across
/// a silent partition, until TCP gives up. Past this the pool is dropped;
/// its connections close when the last of them returns.
pub const CLOSE_BOUND: Duration = Duration::from_secs(1);

/// The worst case from a silent partition of the lock's session to
/// `pipeline_lock: lost` (the lock holder's [`DETECTION`], then the
/// reachability probe the report used to wait for; the loss is now
/// reported at once).
pub const LOST_REPORTED: Duration = DETECTION.saturating_add(PROBE);

// INV-1236: the pipeline has reported the loss and finished abandoning
// before the server can reap the session and hand the lock to another
// process.
const _: () =
    assert!(LOST_REPORTED.saturating_add(LOST_SHUTDOWN).as_millis() < SERVER_REAP.as_millis());

/// How many connections capture's own pool may open: the spool's drainer
/// appends one batch at a time, `/healthz` reads the groups' statistics.
pub const CAPTURE_CONNECTIONS: u32 = 2;

/// What the ops listener and the API read while a pipeline incarnation
/// runs. Set when one starts, cleared when its lock is lost.
#[derive(Debug, Clone, Default)]
pub struct Late {
    pub router: LateRouter,
    pub reporter: Arc<LateValue<LiveReporter>>,
    pub stages: Arc<LateValue<StagesRunning>>,
    pub log_tasks: Arc<LateValue<Tasks>>,
}

impl Late {
    /// Hand the ops listener `live`'s readers.
    fn set_live(&self, live: &Live<PgSet>) {
        self.reporter.set(live.reporter());
        self.stages.set(live.stages_running());
        self.log_tasks.set(live.tasks().clone());
    }

    /// Forget the abandoned incarnation: the API answers `503` (starting),
    /// `/healthz` has no live section and `/readyz` no stage tasks until
    /// the next one runs.
    fn clear(&self) {
        self.router.clear();
        self.reporter.clear();
        self.stages.clear();
        self.log_tasks.clear();
    }
}

/// The API binding the surface is served on, once it exists.
#[derive(Clone)]
pub struct ApiBinding {
    pub tokens: StaticTokens,
    pub clock: Arc<dyn Clock>,
}

/// What the pipeline task hands back when told to stop: the running live
/// process and the lock it holds, `None` when none runs (it never started,
/// or its lock was lost and the next one had not started yet). The live
/// process's pool (`live.layers().pool`) is its incarnation's own: close
/// it once the live process stopped.
pub type PipelineHeld = Option<(Live<PgSet>, PipelineLock)>;

/// The process's Postgres side.
pub struct PgRunning {
    /// The process's template pool: the database probes and the
    /// migrations' head check; each pipeline incarnation's pool is built
    /// from its options.
    pub pool: PgPool,
    /// Capture's own pool, behind [`PgRunning::bus`].
    pub capture_pool: PgPool,
    /// Capture's bus (publish only); the `api` role's bus.
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

    /// [`StopHandle::stop`] by `deadline`: past it the task is aborted
    /// (it was waiting on the database) and `None` returned.
    pub async fn stop_by(self, deadline: Instant) -> Option<T> {
        let _ = self.stop.send(());
        let mut task = self.task;
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(value)) => Some(value),
            Ok(Err(error)) => {
                tracing::error!(error = %error, "a postgres task failed");
                None
            }
            Err(_) => {
                tracing::warn!("a postgres task did not stop by the deadline; aborted");
                task.abort();
                None
            }
        }
    }
}

/// Close `pool` within [`CLOSE_BOUND`]; past it, leave its connections to
/// close as they come back.
pub async fn close_bounded(pool: &PgPool, what: &'static str) {
    if tokio::time::timeout(CLOSE_BOUND, pool.close())
        .await
        .is_err()
    {
        tracing::warn!(
            pool = what,
            bound_ms = u64::try_from(CLOSE_BOUND.as_millis()).unwrap_or(u64::MAX),
            size = pool.size(),
            "a pool did not close in time; its connections close as they return"
        );
    }
}

impl PgRunning {
    /// Stop in dependency order: capture drains its channel (the proxy is
    /// gone), the live process drains and stops, the API surface stops,
    /// the spool's drainer stops (its files stay), the bus and the pools
    /// close. Every step that touches the database is bounded: by
    /// `deadline` (the drains) or [`CLOSE_BOUND`] (each pool's close).
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
            && let Some(Some((live, lock))) = pipeline.stop_by(deadline).await
        {
            let pool = live.layers().pool.clone();
            let live_drained = live.shutdown(deadline).await;
            drained.stages = live_drained.stages;
            drained.log = live_drained.log;
            drop(lock);
            close_bounded(&pool, "pipeline").await;
        }
        if let Some(api) = self.api {
            api.stop_by(deadline).await;
        }
        if let Some(spool) = &self.spool {
            spool.close().await;
        }
        self.bus.shutdown();
        close_bounded(&self.capture_pool, "capture").await;
        close_bounded(&self.pool, "template").await;
        drained
    }
}

/// Build the bus, the spool and the capture pipeline: no database needed.
pub struct CaptureSide {
    /// The template pool the side was built from.
    pub pool: PgPool,
    /// Capture's own pool (the `api` role's bus uses the template pool).
    pub capture_pool: PgPool,
    pub bus: PgBus,
    pub gate: Gate,
    pub spool: Option<LiveBus>,
    pub pipeline: Option<Arc<Pipeline<LiveBlobs, LiveBus>>>,
}

/// A lazy pool on `template`'s database with its options (the acquire
/// timeout) but at most `max` connections, its sessions named
/// `application` in `pg_stat_activity` and set up by
/// [`session_setup`]. It never joins the pipeline fence and runs without
/// the pipeline's statement and lock bounds: capture's publisher pool is
/// one (capture is never fenced, INV-1237; a stalled append spools at the
/// publish's own client-side timeout).
pub fn derived_pool(template: &PgPool, max: u32, application: &str) -> PgPool {
    derived_options(template, max)
        .after_connect(|connection, _meta| Box::pin(session_setup(connection)))
        .connect_lazy_with(derived_connect(template, application))
}

/// [`incarnation_pool_bounded`] with the default bounds
/// ([`PipelineTimeouts::default`]: 60 s per statement, 15 s per lock wait).
pub fn incarnation_pool(template: &PgPool, max: u32, application: &str, epoch: Epoch) -> PgPool {
    incarnation_pool_bounded(
        template,
        max,
        application,
        epoch,
        PipelineTimeouts::default(),
    )
}

/// A pipeline incarnation's pool: [`derived_pool`]'s, each new connection
/// then set to cancel any statement past `timeouts` (`statement_timeout`,
/// `lock_timeout`, for the whole session: a stalled statement is retried,
/// never waited on without end) and joining the pipeline fence as a
/// connection of the incarnation of `epoch` (`store::fence::join`). Once a
/// newer epoch is taken every connect is refused (`FenceError::Fenced`, or
/// `Fencing` while the new holder waits): the pool can never hand out a
/// connection again. `PgPoolOptions::after_connect` replaces the template's
/// hook, so this one runs the session setup first itself.
pub fn incarnation_pool_bounded(
    template: &PgPool,
    max: u32,
    application: &str,
    epoch: Epoch,
    timeouts: PipelineTimeouts,
) -> PgPool {
    let name = application.to_owned();
    derived_options(template, max)
        .after_connect(move |connection, _meta| {
            let name = name.clone();
            Box::pin(async move {
                session_setup(connection).await?;
                timeouts.apply(connection).await?;
                fence::join(connection, epoch).await.map_err(|refused| {
                    tracing::warn!(
                        application_name = %name,
                        epoch = epoch.get(),
                        error = %refused,
                        "a pipeline connection was refused by the fence"
                    );
                    refused.into_sqlx()
                })
            })
        })
        .connect_lazy_with(derived_connect(template, application))
}

/// `template`'s pool options with at most `max` connections.
fn derived_options(template: &PgPool, max: u32) -> PgPoolOptions {
    let options = template.options().clone();
    let min = options.get_min_connections().min(max);
    options.max_connections(max).min_connections(min)
}

/// `template`'s connect options, the sessions named `application`.
fn derived_connect(template: &PgPool, application: &str) -> PgConnectOptions {
    (*template.connect_options())
        .clone()
        .application_name(application)
}

/// The bus over `pool` (for the `api` role) or, for a pipeline role, a
/// publish-only bus over a pool of capture's own, the spool in front of
/// it and the capture pipeline over the spool. `pool` is the process's
/// template pool.
pub async fn capture_side(
    config: &GatewayConfig,
    role: Role,
    pool: PgPool,
    blobs: &FsBlobStore,
    clock: &Arc<dyn Clock>,
) -> Result<CaptureSide, StartError> {
    let section = config.store.ok_or(StartError::NoStore)?;
    let gate = Gate::closed();
    if role == Role::Api {
        // Delays on the wall clock even when the live process runs on a
        // manual one (`live::pg::bus_clock`).
        let bus = PgBus::new(pool.clone(), crate::live::pg::bus_clock(), section.bus)
            .map_err(StartError::Bus)?;
        return Ok(CaptureSide {
            capture_pool: pool.clone(),
            pool,
            bus,
            gate,
            spool: None,
            pipeline: None,
        });
    }
    let capture_pool = derived_pool(
        &pool,
        CAPTURE_CONNECTIONS,
        &format!("crosstalk-capture-{}", std::process::id()),
    );
    let bus = PgBus::publisher(
        capture_pool.clone(),
        crate::live::pg::bus_clock(),
        section.bus,
    )
    .map_err(StartError::Bus)?;
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
        capture_pool,
        bus,
        gate,
        spool: Some(spool),
        pipeline: Some(Arc::new(pipeline)),
    })
}

/// What the pipeline task starts each incarnation from.
pub struct PipelineStart {
    pub config: GatewayConfig,
    pub role: Role,
    pub clock: LiveClock,
    pub blobs: FsBlobStore,
    /// The template pool: probes, the head check, and the options each
    /// incarnation's pool is built with.
    pub pool: PgPool,
    pub url: DatabaseUrl,
    /// Capture's bus: each incarnation's bus takes its config.
    pub bus: PgBus,
    pub spool: LiveBus,
    pub gate: Gate,
    pub pipeline: Arc<Pipeline<LiveBlobs, LiveBus>>,
    pub secret: Arc<KeyedHasher>,
    pub status: StatusReporter,
    pub late: Late,
    pub api: Option<ApiBinding>,
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
        match tokio::time::timeout(HEAD_CHECK, migrations::check_heads(pool)).await {
            Ok(Ok(behind)) if behind.is_empty() => {
                status.update(|status| status.migrations = MigrationState::AtHead);
                return true;
            }
            Ok(Ok(behind)) => {
                let text = migrations::describe(&behind);
                tracing::error!(behind = %text, "migrations are behind; run `crosstalk migrate`");
                status.update(|status| {
                    status.migrations = MigrationState::Behind(text);
                    status.phase = PipelinePhase::WaitingForMigrations;
                });
            }
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "reading the applied migrations failed");
                status.phase(PipelinePhase::WaitingForDatabase);
            }
            Err(_) => {
                tracing::warn!(
                    bound_ms = u64::try_from(HEAD_CHECK.as_millis()).unwrap_or(u64::MAX),
                    "reading the applied migrations did not answer in time"
                );
                status.phase(PipelinePhase::WaitingForDatabase);
            }
        }
        if pause(stop).await {
            return false;
        }
    }
}

/// The status a Postgres-mode process starts with.
pub fn initial_status() -> StatusReporter {
    StatusReporter::new(PipelineStatus::waiting())
}
