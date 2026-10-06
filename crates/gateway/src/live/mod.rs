//! `Live`: the whole detection path and the L8 surface in one process,
//! over one set of in-memory stores. What the UI binary hosts, and what
//! the e2e smoke and the eval harness drive.
//!
//! ```text
//!  proxy capture (optional) ─▶ CaptureStage (L1) ─┐
//!  replay / caller ─▶ pipeline().ingest(exchange, at) ─▶ blobs + ExchangeCaptured
//!                                                  ▼
//!   MpscBus ── one group per slot ──▶ L3 reconstruct ─ ConversationDelta ─▶ L4 provenance ─┬─ ContentMatched ─▶ L5 flow
//!      ▲                                                                   extraction step ─┘ (Extracted, local) ─▶ L5 flow
//!      │                L5 ─ TransmissionConfirmed ─▶ L6 classify ─ TransmissionClassified ─▶ L7 topology
//!      │            ├──▶ evidence feeder ─▶ MemoryEvidence
//!      │            └──▶ surface relay ─▶ node facts, live feed
//!      └── forward_outbox ◀── Outbox ◀── LiveStores (crosstalk-memory), written by every stage
//!   Surface<LiveStores> (crosstalk-api's InProcess, over the same stores, bus and blobs)
//! ```
//!
//! [`Live::start`] opens the blob store and starts the bus, builds the
//! surface over them with `InProcess::start_with`, builds the pipeline,
//! fills every slot ([`wiring::wire_all`]), subscribes every slot's group,
//! and only then starts publishing: the outbox forwarder, the stages, the
//! ticker and the capture stage. Everything time-dependent reads the
//! configured [`LiveClock`], so a corpus or simulation clock replays a
//! dataset through the same path live capture takes, and
//! [`Live::settle`] drives it to a fixed point.

mod blobs;
mod classify;
mod clock;
mod defaults;
mod evidence;
pub mod layers;
mod relay;
mod settle;
mod stage;
pub mod wiring;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crosstalk_api::{Backbone, InProcess, InProcessError, InProcessOptions};
use crosstalk_flow::consumer::{FlowConfig, InvalidFlowConfig, Settings as FlowSettings};
use crosstalk_flow::extract::ExtractConfig;
use crosstalk_memory::support::Outbox;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_reconstruct::thread::ThreadConfig;
use crosstalk_spec::events::Subject;
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::{CallerError, RequestIdentity};
use crosstalk_spec::support::Timestamp;
use crosstalk_surface::Surface;
use crosstalk_transport::blob::OpenError;
use crosstalk_transport::{BusConfig, MpscBus, StartError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use self::blobs::{BlobConfig, LiveBlobs};
pub use self::classify::Classifier;
pub use self::clock::LiveClock;
pub use self::defaults::{DEFAULT_BUCKET, DefaultsError};
pub use self::evidence::{EvidenceFeeder, IndexedSpans, SpanSource, SpanSourceError};
pub use self::settle::{SettleError, Settled};
pub use self::stage::{
    Activity, Command, Control, DERIVED_SUBJECTS, LayerStores, LiveStores, Publisher, RunFuture,
    Slot, SlotTaken, Stage, StageContext, StageError, Stages, settle_delivery,
};
use crate::capture::CaptureStage;
use crate::log::ExchangeLog;
use crate::log::consumer::{self as log_consumer, LogStats};
use crate::pipeline::{BuildError, Deps, Pipeline, Settings};
use crate::tasks::Tasks;

/// The pipeline a live process ingests through.
pub type LivePipeline = Pipeline<LiveBlobs, MpscBus>;

/// When the stages' ticks run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ticking {
    /// Every `flow.tick_ms` of elapsed (tokio) time, reading the clock, and
    /// on every [`Live::settle`]: for a process serving live traffic, or a
    /// harness that polls.
    Periodic,
    /// Only on [`Live::settle`]: nothing time-driven happens between two
    /// settles, so a replay is reproducible.
    OnSettle,
}

/// How a live process is configured.
pub struct LiveConfig {
    /// The surface and its stores. Its `clock` and `timing` are replaced by
    /// [`LiveConfig::clock`] and [`LiveConfig::flow`]'s timing, so every
    /// layer reads one clock and one correlation timing.
    pub surface: InProcessOptions,
    /// The clock every stage reads.
    pub clock: LiveClock,
    pub blobs: BlobConfig,
    pub bus: BusConfig,
    /// Put retries; `consumer_retry` is every slot's group retry policy.
    pub pipeline: Settings,
    /// L5's correlation windows, shards and tick (durations in ms).
    pub flow: FlowConfig,
    /// L4's winnowing, decoding and index settings.
    pub provenance: ProvenanceConfig,
    /// L5's extractors: the MCP tool mapping, the HTTP and fetch tool
    /// names, the site rules.
    pub extract: ExtractConfig,
    /// L3's threading settings: how long a seen message is withheld from a
    /// later conversation's new inputs.
    pub threading: ThreadConfig,
    pub ticking: Ticking,
    /// Seeds every id generator (envelope, agent, conversation ids), so two
    /// runs over the same input mint the same ids.
    pub seed: u64,
    /// The proxy's capture channel, for a process that serves the L0
    /// listener; `None` to ingest only through [`Live::pipeline`].
    pub capture: Option<mpsc::Receiver<RawExchange>>,
    /// The exchange log (the gateway's P3 stopgap), appended by its own
    /// consumer group; `None` to keep no log.
    pub exchange_log: Option<ExchangeLog>,
}

/// Why a live process did not start. Nothing it spawned keeps running
/// except what an `InProcess` that started leaves until it is dropped.
#[derive(Debug, thiserror::Error)]
pub enum LiveError {
    #[error("the flow config is invalid: {0}")]
    Flow(#[from] InvalidFlowConfig),
    #[error("the blob store did not open: {0}")]
    Blobs(#[from] OpenError),
    #[error("the bus did not start: {0:?}")]
    Bus(StartError),
    #[error("the surface did not start: {0}")]
    Surface(#[from] InProcessError),
    #[error("the pipeline did not build: {0}")]
    Pipeline(#[from] BuildError),
    #[error(transparent)]
    Slot(#[from] SlotTaken),
    #[error("the {} slot did not subscribe: {error:?}", slot.name())]
    Subscribe { slot: Slot, error: BusError },
    #[error("the exchange log did not subscribe: {0:?}")]
    LogSubscribe(BusError),
}

/// How a live process drained on shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveDrained {
    /// The capture stage handled every queued exchange in time (true when
    /// it does not run).
    pub capture: bool,
    /// Every slot's group was empty before the bus stopped.
    pub stages: bool,
    /// The exchange log consumed every published envelope in time (true
    /// when it does not run).
    pub log: bool,
}

/// A running stage: its task, where its commands go, and its count.
struct Running {
    slot: Slot,
    task: JoinHandle<()>,
    commands: mpsc::UnboundedSender<Command>,
    activity: Activity,
}

/// What a live process reports for `/healthz`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct LiveReport {
    /// Deliveries and side inputs each stage handled, by slot name.
    pub stages: BTreeMap<String, u64>,
    /// The L7 watermark, in microseconds since the epoch (0 until it first
    /// advances).
    pub watermark_micros: u64,
}

/// A running live process.
pub struct Live {
    pipeline: Arc<LivePipeline>,
    backend: InProcess<LiveBlobs, LayerStores>,
    context: StageContext,
    clock: LiveClock,
    activity: Activity,
    stages: Vec<Running>,
    outbox: JoinHandle<()>,
    flushes: mpsc::UnboundedSender<relay::Flush>,
    ticker: Option<JoinHandle<()>>,
    capture: Option<JoinHandle<()>>,
    exchange_log: Option<JoinHandle<()>>,
    log_stats: Arc<LogStats>,
    /// `exchange_log` and `capture` when they run, for `/readyz`.
    tasks: Tasks,
    /// One flag per stage task, for `/readyz`'s `live`.
    stage_tasks: Tasks,
}

impl Live {
    /// Start everything; see the module docs for the order. Needs a tokio
    /// runtime.
    pub async fn start(config: LiveConfig) -> Result<Self, LiveError> {
        let LiveConfig {
            mut surface,
            clock,
            blobs,
            bus,
            pipeline,
            flow,
            provenance,
            extract,
            threading,
            ticking,
            seed,
            capture,
            exchange_log,
        } = config;
        let flow = FlowSettings::try_from(flow)?;
        let reader = clock.reader();
        surface.clock = Arc::clone(&reader);
        surface.timing = flow.timing;
        let blobs = LiveBlobs::open(&blobs).await?;
        let bus = MpscBus::start(bus).map_err(LiveError::Bus)?;
        let (outbox, outboxed) = Outbox::channel();
        let (relay_events, relayed) = mpsc::unbounded_channel();
        let layers = LayerStores::new(threading);
        let backend = InProcess::start_with_reads(
            surface,
            Backbone {
                bus: bus.clone(),
                blobs: blobs.clone(),
                outbox,
                events: relayed,
            },
            layers.clone(),
        )
        .await?;
        let built = Pipeline::build(
            pipeline,
            Deps::stores(blobs, bus.clone(), SeededRandom::new(seed)),
            Arc::clone(&reader),
        )
        .await?;
        let publisher = Publisher::new(built.ingester());
        let activity = Activity::default();
        let context = StageContext {
            stores: backend.stores.clone(),
            layers,
            publisher: publisher.clone(),
            clock: reader,
            flow,
            seed,
            watermark: Arc::new(AtomicU64::new(0)),
        };
        let mut stages = Stages::default();
        wiring::wire_all(&mut stages, &context, &provenance, &extract)?;
        stages.fill(Slot::SurfaceRelay, relay::SurfaceRelay::new(relay_events))?;
        for slot in stages.unfilled() {
            tracing::warn!(slot = slot.name(), "slot unfilled: its layer does not run");
        }
        // Every group subscribes before anything below can publish.
        let log_subscription = match &exchange_log {
            Some(_) => Some(
                bus.subscribe(
                    &[Subject::ExchangeCaptured],
                    log_consumer::group(),
                    pipeline.consumer_retry,
                )
                .await
                .map_err(LiveError::LogSubscribe)?,
            ),
            None => None,
        };
        let mut subscribed = Vec::new();
        for (slot, plug) in stages.into_plugs() {
            let subscription = bus
                .subscribe(&plug.subjects, slot.group(), pipeline.consumer_retry)
                .await
                .map_err(|error| LiveError::Subscribe { slot, error })?;
            subscribed.push((slot, plug, subscription));
        }
        let mut stage_tasks = Tasks::new();
        let stages: Vec<Running> = subscribed
            .into_iter()
            .map(|(slot, plug, subscription)| {
                let (commands, received) = mpsc::unbounded_channel();
                let own = activity.stage();
                let control = Control {
                    retry: pipeline.consumer_retry,
                    commands: received,
                    activity: own.clone(),
                    slot,
                };
                Running {
                    slot,
                    task: stage_tasks.spawn(slot.name(), (plug.run)(subscription, control)),
                    commands,
                    activity: own,
                }
            })
            .collect();
        let mut tasks = Tasks::new();
        let log_stats = Arc::new(LogStats::new());
        let exchange_log = match (exchange_log, log_subscription) {
            (Some(log), Some(subscription)) => Some(tasks.spawn(
                "exchange_log",
                log_consumer::run(subscription, log, Arc::clone(&log_stats)),
            )),
            _ => None,
        };
        let (flushes, flush_requests) = mpsc::unbounded_channel();
        let outbox = tokio::spawn(relay::forward_outbox(
            outboxed,
            publisher,
            flush_requests,
            activity.clone(),
        ));
        let ticker = match ticking {
            Ticking::Periodic => Some(tokio::spawn(settle::tick_periodically(
                settle::commands_of(&stages),
                clock.clone(),
                flow.tick_every,
            ))),
            Ticking::OnSettle => None,
        };
        let capture = capture.map(|captured| {
            tasks.spawn("capture", CaptureStage::new(built.ingester()).run(captured))
        });
        tracing::info!(
            stages = ?stages.iter().map(|running| running.slot.name()).collect::<Vec<_>>(),
            capture = capture.is_some(),
            ticking = ?ticking,
            "live process started"
        );
        Ok(Self {
            pipeline: Arc::new(built),
            backend,
            context,
            clock,
            activity,
            stages,
            outbox,
            flushes,
            ticker,
            capture,
            exchange_log,
            log_stats,
            tasks,
            stage_tasks,
        })
    }

    /// The process's long-running side tasks (`exchange_log`, `capture`)
    /// and their running flags. A clone shares the flags.
    pub fn tasks(&self) -> &Tasks {
        &self.tasks
    }

    /// Whether every stage task is still running.
    pub fn stages_running(&self) -> StagesRunning {
        StagesRunning(self.stage_tasks.clone())
    }

    /// The exchange log consumer's counters.
    pub fn log_stats(&self) -> &Arc<LogStats> {
        &self.log_stats
    }

    /// The L7 watermark the topology stage last exposed.
    pub fn watermark(&self) -> Timestamp {
        Timestamp::from_micros(self.context.watermark.load(Ordering::SeqCst))
    }

    /// The `/healthz` section: each stage's handled count and the
    /// watermark.
    pub fn report(&self) -> LiveReport {
        LiveReport {
            stages: self
                .stages
                .iter()
                .map(|running| (running.slot.name().to_owned(), running.activity.handled()))
                .collect(),
            watermark_micros: self.context.watermark.load(Ordering::SeqCst),
        }
    }

    /// A cloneable reader of [`Live::report`], for the ops listener.
    pub fn reporter(&self) -> LiveReporter {
        LiveReporter {
            stages: self
                .stages
                .iter()
                .map(|running| (running.slot, running.activity.clone()))
                .collect(),
            watermark: Arc::clone(&self.context.watermark),
        }
    }

    /// Where exchanges enter: `pipeline().ingest(exchange, at)` or
    /// `pipeline().ingester()` for another task.
    pub fn pipeline(&self) -> &Arc<LivePipeline> {
        &self.pipeline
    }

    /// The surface the UI reads: `QueryApi`, `OperatorActions`, `LiveFeed`.
    pub fn surface(&self) -> &Arc<Surface<LiveStores>> {
        &self.backend.surface
    }

    /// The stores every stage and the surface share: read them through the
    /// spec's traits (`TransmissionStore::list` on `transmissions`, ...).
    pub fn stores(&self) -> &LiveStores {
        &self.backend.stores
    }

    /// The layer stores only the stages read: L3's conversations
    /// (`ExchangePlacements::placement`) and L4's provenance records.
    pub fn layers(&self) -> &LayerStores {
        &self.context.layers
    }

    /// The stores, publisher and clock the stages were built from.
    pub fn context(&self) -> &StageContext {
        &self.context
    }

    /// The clock every stage reads.
    pub fn clock(&self) -> &LiveClock {
        &self.clock
    }

    /// The slots that run, in slot order.
    pub fn filled(&self) -> Vec<Slot> {
        self.stages.iter().map(|running| running.slot).collect()
    }

    /// The caller of one request, from the loaded access config.
    pub async fn caller(&self, identity: RequestIdentity) -> Result<Caller, CallerError> {
        self.backend.caller(identity).await
    }

    /// Drain and stop by `deadline`: the capture stage handles what is
    /// queued (the caller has dropped every capture sender), every slot's
    /// group empties, the bus stops, the stages end, the surface's relay
    /// and live feed stop.
    pub async fn shutdown(self, deadline: Instant) -> LiveDrained {
        if let Some(ticker) = &self.ticker {
            ticker.abort();
        }
        let capture = match self.capture {
            Some(task) => join_by("capture stage", task, deadline).await,
            None => true,
        };
        let bus = self.backend.stores.bus.clone();
        let slots: Vec<Slot> = self.stages.iter().map(|running| running.slot).collect();
        let drained = match tokio::time::timeout_at(deadline, settle::idle(&bus, &slots)).await {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "reading the stages' groups failed");
                false
            }
            Err(_) => {
                tracing::warn!("the stages did not catch up before the deadline");
                false
            }
        };
        let log = match self.exchange_log.is_some() {
            true => {
                let group = log_consumer::group();
                let empty = settle::group_idle(&bus, &group);
                matches!(tokio::time::timeout_at(deadline, empty).await, Ok(Ok(())))
            }
            false => true,
        };
        bus.shutdown().await;
        for running in self.stages {
            join_by(running.slot.name(), running.task, deadline).await;
        }
        if let Some(task) = self.exchange_log {
            join_by("exchange log consumer", task, deadline).await;
        }
        self.outbox.abort();
        self.backend.shutdown().await;
        tracing::info!(capture, stages = drained, log, "live process stopped");
        LiveDrained {
            capture,
            stages: drained,
            log,
        }
    }
}

/// Whether every stage task of a live process still runs; cloneable, for
/// `/readyz`.
#[derive(Debug, Clone)]
pub struct StagesRunning(Tasks);

impl StagesRunning {
    pub fn all(&self) -> bool {
        self.0.states().iter().all(|(_, running)| *running)
    }
}

/// A cloneable reader of a live process's [`LiveReport`], for `/healthz`.
#[derive(Debug, Clone)]
pub struct LiveReporter {
    stages: Vec<(Slot, Activity)>,
    watermark: Arc<AtomicU64>,
}

impl LiveReporter {
    pub fn report(&self) -> LiveReport {
        LiveReport {
            stages: self
                .stages
                .iter()
                .map(|(slot, activity)| (slot.name().to_owned(), activity.handled()))
                .collect(),
            watermark_micros: self.watermark.load(Ordering::SeqCst),
        }
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
                "task did not finish before the deadline; aborting"
            );
            task.abort();
            false
        }
    }
}

/// How long the idle checks wait between two reads of the groups.
const POLL: Duration = Duration::from_millis(1);
