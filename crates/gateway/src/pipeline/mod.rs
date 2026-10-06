//! The pipeline: the gateway's composition of the layers behind the proxy,
//! as a library entry point.
//!
//! ```text
//! RawExchange ─ capture channel ─▶ CaptureStage (L1 normalize) ─┐
//!                                                               ▼
//!             NormalizedExchange, at ─────────────────▶ Pipeline::ingest
//!                                                         │ store blobs (retried) ─▶ BlobStore
//!                                                         │ mint EventId at `at`
//!                                                         ▼ publish
//!                                  EventBus ── ExchangeCaptured ──▶ group exchange-log ─▶ ExchangeLog
//! ```
//!
//! [`Pipeline::build`] takes the [`Settings`], the stores and stages in
//! [`Deps`], and the injected [`Clock`]. It subscribes every consumer
//! stage before anything can publish, then spawns the stages it was given:
//! the exchange log's consumer (task `exchange_log`) and the capture stage
//! over the proxy's channel (task `capture`). The `crosstalk serve` roles
//! build one (see [`crate::gateway`]); the eval harness builds one over the
//! simulation's stores and clock and calls [`Pipeline::ingest`] with
//! pre-normalized exchanges. Everything time-dependent reads the injected
//! clock or tokio time, so a pipeline runs under paused time.
//!
//! Shutdown, in dependency order: the capture channel's senders are
//! dropped by the caller, the capture stage drains it
//! ([`Pipeline::join_capture`]), the consumers' groups drain and the bus
//! stops, and the consumers end ([`Pipeline::join_consumers`]).
//! [`Pipeline::shutdown`] does all of it for the in-process bus.

mod ingest;
mod stats;

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::events::Subject;
use crosstalk_spec::ids::{EventId, SeededRandom};
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l1_canonical::NormalizedExchange;
use crosstalk_spec::interfaces::l2_transport::{BlobStore, BusError, EventBus, RetryPolicy};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_transport::{BusConfig, MpscBus};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use self::ingest::{Bodies, IngestError, Ingester, PublishError};
pub use self::stats::{PipelineCounts, PipelineStats, PutRetry};
use crate::capture::CaptureStage;
use crate::config::GatewayConfig;
use crate::log::ExchangeLog;
use crate::log::consumer::{self, LogStats};
use crate::tasks::Tasks;

/// How the pipeline retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    /// Blob puts, per exchange.
    pub put_retry: PutRetry,
    /// Bus redelivery for the consumer stages' groups.
    pub consumer_retry: RetryPolicy,
}

impl Settings {
    /// The `pipeline` and `bus` sections of the gateway's config.
    pub fn from_config(config: &GatewayConfig) -> Self {
        Self {
            put_retry: PutRetry::from(config.pipeline),
            consumer_retry: config.bus.retry,
        }
    }
}

impl Default for Settings {
    /// The config's defaults: three put attempts 100 ms apart, the bus's
    /// default retry policy.
    fn default() -> Self {
        Self {
            put_retry: PutRetry::default(),
            consumer_retry: BusConfig::default().retry,
        }
    }
}

/// What the pipeline runs over, and which stages it runs.
pub struct Deps<B, E> {
    pub blobs: B,
    pub bus: E,
    /// Seeds the random part of envelope ids: `SeededRandom::from_entropy`
    /// in the gateway, a fixed seed under simulation.
    pub id_entropy: SeededRandom,
    /// The proxy's capture channel, for a role that runs the proxy: the
    /// capture stage normalizes what arrives and ingests it.
    pub capture: Option<mpsc::Receiver<RawExchange>>,
    /// The exchange log, for a role that runs the bus consumers.
    pub exchange_log: Option<ExchangeLog>,
    /// Which message bodies an ingest puts (`Bodies::PutEvery` unless
    /// the blob store never drops a body).
    pub bodies: Bodies,
}

impl<B, E> Deps<B, E> {
    /// Just the stores: a pipeline fed only through [`Pipeline::ingest`].
    pub fn stores(blobs: B, bus: E, id_entropy: SeededRandom) -> Self {
        Self {
            blobs,
            bus,
            id_entropy,
            capture: None,
            exchange_log: None,
            bodies: Bodies::PutEvery,
        }
    }
}

/// Why the pipeline was not built. Nothing it would have spawned is
/// running.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error("subscribing the exchange log: {0:?}")]
    Subscribe(BusError),
}

/// How the pipeline drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Drained {
    /// Whether the capture stage handled every queued exchange in time
    /// (true when it does not run).
    pub capture: bool,
    /// Whether the exchange log consumed every published envelope in time
    /// (true when it does not run).
    pub log: bool,
}

/// A built pipeline: the ingest path, and the stage tasks it spawned.
#[derive(Debug)]
pub struct Pipeline<B, E> {
    ingest: Ingester<B, E>,
    log_stats: Arc<LogStats>,
    tasks: Tasks,
    capture: Option<JoinHandle<()>>,
    exchange_log: Option<JoinHandle<()>>,
}

impl<B, E> Pipeline<B, E>
where
    B: BlobStore + Send + Sync + 'static,
    E: EventBus + Send + Sync + 'static,
{
    /// Wire the stages over `deps`, stamping with `clock`. Must be called
    /// inside a tokio runtime (it spawns the stages).
    pub async fn build(
        settings: Settings,
        deps: Deps<B, E>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self, BuildError> {
        let Deps {
            blobs,
            bus,
            id_entropy,
            capture,
            exchange_log,
            bodies,
        } = deps;
        // Subscribed before the capture stage exists, so no envelope is
        // published before every consumer's group does.
        let subscription = match &exchange_log {
            Some(_) => Some(
                bus.subscribe(
                    &[Subject::ExchangeCaptured],
                    consumer::group(),
                    settings.consumer_retry,
                )
                .await
                .map_err(BuildError::Subscribe)?,
            ),
            None => None,
        };
        let ingest = Ingester::new(
            blobs,
            bus,
            clock,
            id_entropy,
            settings.put_retry,
            Arc::new(PipelineStats::new()),
            bodies,
        );
        let log_stats = Arc::new(LogStats::new());
        let mut tasks = Tasks::new();
        let exchange_log = match (exchange_log, subscription) {
            (Some(log), Some(subscription)) => Some(tasks.spawn(
                "exchange_log",
                consumer::run(subscription, log, Arc::clone(&log_stats)),
            )),
            _ => None,
        };
        let capture = capture.map(|captured| {
            tasks.spawn("capture", CaptureStage::new(ingest.clone()).run(captured))
        });
        Ok(Self {
            ingest,
            log_stats,
            tasks,
            capture,
            exchange_log,
        })
    }

    /// Ingest a pre-normalized exchange at `at`: store its blobs (retried),
    /// mint its envelope id at `at`, publish `ExchangeCaptured`. The path
    /// the capture stage takes after normalizing.
    pub async fn ingest(
        &self,
        exchange: NormalizedExchange,
        at: Timestamp,
    ) -> Result<EventId, IngestError> {
        self.ingest.ingest(exchange, at).await
    }

    /// A handle on the ingest path, for callers on other tasks.
    pub fn ingester(&self) -> Ingester<B, E> {
        self.ingest.clone()
    }

    pub fn blobs(&self) -> &B {
        self.ingest.blobs()
    }

    pub fn bus(&self) -> &E {
        self.ingest.bus()
    }

    pub fn stats(&self) -> &Arc<PipelineStats> {
        self.ingest.stats()
    }

    pub fn log_stats(&self) -> &Arc<LogStats> {
        &self.log_stats
    }

    /// The stage tasks' running flags (`exchange_log`, `capture`), for
    /// `/readyz`. A clone shares the flags; spawning onto it adds tasks to
    /// the clone only.
    pub fn tasks(&self) -> &Tasks {
        &self.tasks
    }

    /// Wait until `deadline` for the capture stage to drain its channel,
    /// which ends once every sender is dropped; abort it after that. True
    /// when it ended on its own or does not run.
    pub async fn join_capture(&mut self, deadline: Instant) -> bool {
        match self.capture.take() {
            Some(capture) => join_by("capture stage", capture, deadline).await,
            None => true,
        }
    }

    /// Wait until `deadline` for the consumer stages to end, which they do
    /// once the bus has shut down; abort them after that.
    pub async fn join_consumers(&mut self, deadline: Instant) -> bool {
        match self.exchange_log.take() {
            Some(log) => join_by("exchange log consumer", log, deadline).await,
            None => true,
        }
    }
}

impl<B> Pipeline<B, MpscBus>
where
    B: BlobStore + Send + Sync + 'static,
{
    /// Drain and stop, by `deadline`: the capture stage handles what is
    /// queued (the caller has dropped every capture sender), the exchange
    /// log's group empties, the bus stops, the consumers end and the log is
    /// closed.
    pub async fn shutdown(mut self, deadline: Instant) -> Drained {
        let capture = self.join_capture(deadline).await;
        let log = match self.exchange_log.is_some() {
            true => {
                let drained = wait_for_group(self.bus(), deadline).await;
                self.bus().shutdown().await;
                self.join_consumers(deadline).await;
                drained
            }
            false => {
                self.bus().shutdown().await;
                true
            }
        };
        Drained { capture, log }
    }
}

/// Wait for `task` until `deadline`; abort it after that. True when it
/// ended on its own.
async fn join_by(name: &'static str, mut task: JoinHandle<()>, deadline: Instant) -> bool {
    match tokio::time::timeout_at(deadline, &mut task).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::error!(task = name, error = %error, "task failed");
            false
        }
        Err(_) => {
            tracing::warn!(
                task = name,
                "task did not finish before the flush deadline; aborting"
            );
            task.abort();
            false
        }
    }
}

/// Wait until the exchange log's group holds nothing, until `deadline`.
async fn wait_for_group(bus: &MpscBus, deadline: Instant) -> bool {
    let group = consumer::group();
    let empty = async {
        loop {
            match bus.depth(&group).await {
                Ok(Some(depth))
                    if depth.ready
                        + depth.delayed
                        + depth.held
                        + depth.exhausted
                        + depth.waiting
                        > 0 => {}
                Ok(_) => return true,
                Err(error) => {
                    tracing::warn!(error = ?error, "reading the exchange log's group depth failed");
                    return false;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    match tokio::time::timeout_at(deadline, empty).await {
        Ok(drained) => drained,
        Err(_) => {
            tracing::warn!("the exchange log did not catch up before the flush deadline");
            false
        }
    }
}
