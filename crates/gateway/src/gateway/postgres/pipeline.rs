//! The pipeline task: one loop for the process's life, through any number
//! of pipeline-lock losses.
//!
//! ```text
//!            ┌──────────────────────────────────────────────────────────────┐
//!            ▼                                                              │
//! Waiting ─ database, migrations, lock ─▶ Starting{lock} ─ start_pg ─▶ Running(Incarnation)
//!   ▲   (held elsewhere: retried)            │ lost / failed: parts abandoned      │ lost
//!   │                                        ▼                                     ▼
//!   └──────────────────────────────── (waiting again) ◀──── Abandoning{incarnation, loss}
//! ```
//!
//! Each [`State::Starting`] builds incarnation `n`: a pool of its own
//! (`application_name` `crosstalk-pipeline-<pid>-<n>`, so its sessions are
//! told apart in `pg_stat_activity`) and a `PgBus` over it, whose
//! listener, reaper and takes belong to it. A lost lock abandons the
//! incarnation and never drains it: the status reads `lost` /
//! `abandoning` at once, the API and the ops handles are cleared, the pool
//! is closed without waiting (the fence: every acquire fails from then on,
//! and a connection an aborted task gives back is closed, not pinged),
//! every task is aborted and joined (`Live::abandon`, `PgBus::abandon`),
//! and the loop waits for the database and the lock again. What the next
//! incarnation's recovery finds (held deliveries, outbox rows, the last
//! checkpoint) is what a killed process leaves, which recovery already
//! handles: `recover_held` readies the abandoned holds at once.
//!
//! Capture never notices: it appends through the spool and a bus and pool
//! of its own, and the proxy keeps listening.

use std::sync::Arc;

use crosstalk_api::http::{Auth, HttpApi, HttpConfig};
use crosstalk_store::SerializableRetry;
use crosstalk_store::sqlx::PgPool;
use crosstalk_transport::PgBus;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::{
    LOST_SHUTDOWN, PipelineHeld, PipelineStart, SERVE_IDS, StopHandle, close_bounded, derived_pool,
    pause, wait_for_database, wait_for_migrations,
};
use crate::gateway::{StartError, access};
use crate::live::pg::{PgParts, PgSet, bus_clock};
use crate::live::recovery::{LastLoss, LockState, PipelinePhase};
use crate::live::{BlobConfig, Live, LiveBlobs, LiveConfig, Ticking};
use crate::log::ExchangeLog;
use crate::pipeline::Settings;
use crate::store::lock::{LockLoss, PipelineLock};

/// Run the pipeline task: see the module docs.
pub fn spawn_pipeline(start: PipelineStart) -> StopHandle<PipelineHeld> {
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(run_pipeline(start, stopped));
    StopHandle { stop, task }
}

/// Where the pipeline task is (see the module docs).
enum State {
    /// Waiting for the database, migrations at head and the lock.
    Waiting,
    /// The lock is held; the next incarnation starts under it.
    Starting { lock: PipelineLock },
    /// An incarnation runs until shutdown or its lock's loss.
    Running(Incarnation),
    /// The running incarnation's lock was lost: abandon it, then wait.
    Abandoning {
        incarnation: Incarnation,
        loss: LockLoss,
    },
}

/// One run of the pipeline under one taking of the lock. Its pool and bus
/// (`live.layers()`) were built for it and are never reused.
struct Incarnation {
    number: u64,
    live: Live<PgSet>,
    lock: PipelineLock,
}

/// An incarnation's own pool and bus, built before its live process.
struct Parts {
    number: u64,
    pool: PgPool,
    bus: PgBus,
}

/// The `application_name` of incarnation `number`'s sessions.
pub fn application_name(number: u64) -> String {
    format!("crosstalk-pipeline-{}-{number}", std::process::id())
}

impl Parts {
    fn new(start: &PipelineStart, number: u64) -> Result<Self, StartError> {
        let pool = derived_pool(
            &start.pool,
            start.pool.options().get_max_connections(),
            &application_name(number),
        );
        // Delays on the wall clock even when the live process runs on a
        // manual one (`live::pg::bus_clock`).
        let bus =
            PgBus::new(pool.clone(), bus_clock(), *start.bus.config()).map_err(StartError::Bus)?;
        Ok(Self { number, pool, bus })
    }

    /// Abandon parts no live process runs over (its start was cut short or
    /// failed): the pool fenced, the bus's tasks and takes joined.
    async fn abandon(self, deadline: Instant) {
        fence(&self.pool);
        let abandoned = self.bus.abandon(deadline).await;
        tracing::info!(
            incarnation = self.number,
            tasks_joined = abandoned.tasks_joined,
            takes_joined = abandoned.takes_joined,
            "a pipeline incarnation that never ran was abandoned"
        );
    }
}

/// Close `pool` for good without waiting for it: `close` marks the pool
/// closed as it is called, before its future is polled, so from here on
/// every acquire fails with `PoolClosed` and a connection an aborted task
/// gives back is closed instead of pinged (a ping across a silent
/// partition never answers). The close itself, which waits for every
/// connection, runs bounded in a task of its own.
fn fence(pool: &PgPool) {
    let _marked = pool.close();
    let pool = pool.clone();
    tokio::spawn(async move { close_bounded(&pool, "abandoned pipeline").await });
}

async fn run_pipeline(start: PipelineStart, mut stop: oneshot::Receiver<()>) -> PipelineHeld {
    let mut incarnations = 0_u64;
    let mut state = State::Waiting;
    loop {
        state = match state {
            State::Waiting => match wait_for_lock(&start, &mut stop).await {
                Some(lock) => State::Starting { lock },
                None => return None,
            },
            State::Starting { lock } => {
                incarnations += 1;
                match start_incarnation(&start, incarnations, lock, &mut stop).await {
                    Started::Running(incarnation) => State::Running(*incarnation),
                    Started::Lost => State::Waiting,
                    Started::Failed(StartFailed::Retry) => match pause(&mut stop).await {
                        true => return None,
                        false => State::Waiting,
                    },
                    Started::Failed(StartFailed::Permanent) => {
                        let _ = stop.await;
                        return None;
                    }
                    Started::Stopped => return None,
                }
            }
            State::Running(mut incarnation) => tokio::select! {
                _ = &mut stop => return Some((incarnation.live, incarnation.lock)),
                loss = incarnation.lock.lost() => {
                    // The lock's session failed or went silent: a reset
                    // connection, a partition or a database outage.
                    report_loss(&start, incarnation.number, &loss);
                    State::Abandoning { incarnation, loss }
                }
            },
            State::Abandoning { incarnation, loss } => {
                abandon(&start, incarnation, &loss).await;
                State::Waiting
            }
        };
    }
}

/// Wait for the database, migrations at head and the lock; `None` when
/// told to stop.
async fn wait_for_lock(
    start: &PipelineStart,
    stop: &mut oneshot::Receiver<()>,
) -> Option<PipelineLock> {
    let status = &start.status;
    loop {
        if !wait_for_database(&start.pool, status, stop).await
            || !wait_for_migrations(&start.pool, status, stop).await
        {
            return None;
        }
        match PipelineLock::try_take(&start.url).await {
            Ok(Some(lock)) => return Some(lock),
            Ok(None) => {
                tracing::warn!("the pipeline lock is held by another process; waiting");
                status.update(|status| {
                    status.lock = LockState::HeldElsewhere;
                    status.phase = PipelinePhase::WaitingForLock;
                });
            }
            Err(error) => tracing::warn!(error = %error, "taking the pipeline lock failed"),
        }
        if pause(stop).await {
            return None;
        }
    }
}

/// How an incarnation's start ended.
enum Started {
    Running(Box<Incarnation>),
    /// The lock was lost during the start: reported, the start dropped and
    /// its parts abandoned.
    Lost,
    /// The start failed; its parts are abandoned and the lock let go.
    Failed(StartFailed),
    /// Told to stop during the start: its parts are abandoned.
    Stopped,
}

/// Why the live process did not start: [`StartFailed::Permanent`] waits
/// for shutdown, [`StartFailed::Retry`] tries again after a pause.
enum StartFailed {
    Permanent,
    Retry,
}

/// Start incarnation `number` under `lock`. The loss is watched from the
/// moment the lock is taken, so `pipeline_lock` never reads `held` past
/// the detection bound: a start the loss (or a stop) cuts short is
/// dropped, its parts abandoned.
async fn start_incarnation(
    start: &PipelineStart,
    number: u64,
    mut lock: PipelineLock,
    stop: &mut oneshot::Receiver<()>,
) -> Started {
    let parts = match Parts::new(start, number) {
        Ok(parts) => parts,
        Err(error) => {
            tracing::error!(error = %error, "the pipeline's bus did not start");
            start
                .status
                .phase(PipelinePhase::Stopped(error.to_string()));
            return Started::Failed(StartFailed::Permanent);
        }
    };
    start.status.update(|status| {
        status.lock = LockState::Held;
        status.report.incarnation = number;
    });
    tracing::info!(
        incarnation = number,
        application_name = %application_name(number),
        "pipeline incarnation starting"
    );
    let cut = {
        let starting = start_live(start, &parts);
        tokio::pin!(starting);
        tokio::select! {
            started = &mut starting => Ok(started),
            loss = lock.lost() => Err(Some(loss)),
            _ = &mut *stop => Err(None),
        }
    };
    match cut {
        Ok(Ok(live)) => Started::Running(Box::new(Incarnation { number, live, lock })),
        Ok(Err(failed)) => {
            parts.abandon(Instant::now() + LOST_SHUTDOWN).await;
            drop(lock);
            Started::Failed(failed)
        }
        Err(Some(loss)) => {
            report_loss(start, number, &loss);
            start.late.clear();
            parts.abandon(Instant::now() + LOST_SHUTDOWN).await;
            drop(lock);
            start.status.phase(PipelinePhase::WaitingForDatabase);
            Started::Lost
        }
        Err(None) => {
            parts.abandon(Instant::now() + LOST_SHUTDOWN).await;
            drop(lock);
            Started::Stopped
        }
    }
}

/// Report the lock lost, at once: `pipeline_lock: lost`, `pipeline:
/// abandoning`, the loss counted. Within the lock's `DETECTION` of the
/// session going silent (INV-1236).
fn report_loss(start: &PipelineStart, number: u64, loss: &LockLoss) {
    let at_micros = start.clock.reader().now().as_micros();
    tracing::error!(
        cause = "pipeline_lock_lost",
        reason = loss.reason(),
        session = %loss,
        incarnation = number,
        "the pipeline lock's session failed; the incarnation is abandoned and the pipeline \
         resumes once the database answers and the lock is free"
    );
    start.status.update(|status| {
        status.lock = LockState::Lost;
        status.phase = PipelinePhase::Abandoning;
        status.report.lock_losses = status.report.lock_losses.saturating_add(1);
        status.report.last_loss = Some(LastLoss {
            reason: loss.kind(),
            at_micros,
        });
    });
}

/// Abandon a running incarnation whose lock was lost (see the module
/// docs), within [`LOST_SHUTDOWN`].
async fn abandon(start: &PipelineStart, incarnation: Incarnation, loss: &LockLoss) {
    let began = Instant::now();
    let Incarnation { number, live, lock } = incarnation;
    start.late.clear();
    let pool = live.layers().pool.clone();
    fence(&pool);
    let abandoned = live.abandon(began + LOST_SHUTDOWN).await;
    drop(lock);
    tracing::warn!(
        incarnation = number,
        reason = loss.reason(),
        joined = abandoned.joined(),
        tasks_joined = abandoned.tasks_joined,
        bus_joined = abandoned.bus_joined,
        took_ms = u64::try_from(began.elapsed().as_millis()).unwrap_or(u64::MAX),
        "pipeline incarnation abandoned; waiting for the database and the lock"
    );
    start.status.phase(PipelinePhase::WaitingForDatabase);
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

/// Start the live process over `parts` and hand the API and the ops
/// listener its readers.
async fn start_live(start: &PipelineStart, parts: &Parts) -> Result<Live<PgSet>, StartFailed> {
    let status = &start.status;
    let config = match live_config(start).await {
        Ok(config) => config,
        Err(error) => {
            status.phase(PipelinePhase::Stopped(error.to_string()));
            tracing::error!(error = %error, "the live process's config failed");
            return Err(StartFailed::Permanent);
        }
    };
    let started = Live::start_pg(
        config,
        PgParts {
            pool: parts.pool.clone(),
            bus: parts.bus.clone(),
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
            return Err(match error.is_permanent() {
                true => StartFailed::Permanent,
                false => StartFailed::Retry,
            });
        }
    };
    start.late.set_live(&live);
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
            Ok(None) => {
                tracing::error!("no operator directory stored; the API stays unavailable")
            }
            Err(error) => {
                tracing::error!(error = ?error, "reading the operator directory failed; the API stays unavailable");
            }
        }
    }
    Ok(live)
}
