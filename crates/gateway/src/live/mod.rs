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
mod evidence;
pub mod layers;
mod relay;
mod settle;
mod stage;
pub mod wiring;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::{Backbone, InProcess, InProcessError, InProcessOptions};
use crosstalk_flow::consumer::{FlowConfig, InvalidFlowConfig, Settings as FlowSettings};
use crosstalk_memory::support::Outbox;
use crosstalk_provenance::config::ProvenanceConfig;
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus};
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::operators::{CallerError, RequestIdentity};
use crosstalk_surface::Surface;
use crosstalk_transport::blob::OpenError;
use crosstalk_transport::{BusConfig, MpscBus, StartError};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use self::blobs::{BlobConfig, LiveBlobs};
pub use self::classify::Classifier;
pub use self::clock::LiveClock;
pub use self::evidence::{EvidenceFeeder, ProvenanceSpans, SpanSource, SpanSourceError};
pub use self::settle::{SettleError, Settled};
pub use self::stage::{
    Activity, Command, Control, DERIVED_SUBJECTS, LayerStores, LiveStores, Publisher, RunFuture,
    Slot, SlotTaken, Stage, StageContext, StageError, Stages, settle_delivery,
};
use crate::capture::CaptureStage;
use crate::pipeline::{BuildError, Deps, Pipeline, Settings};

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
    pub ticking: Ticking,
    /// Seeds every id generator (envelope, agent, conversation ids), so two
    /// runs over the same input mint the same ids.
    pub seed: u64,
    /// The proxy's capture channel, for a process that serves the L0
    /// listener; `None` to ingest only through [`Live::pipeline`].
    pub capture: Option<mpsc::Receiver<RawExchange>>,
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
}

/// How a live process drained on shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveDrained {
    /// The capture stage handled every queued exchange in time (true when
    /// it does not run).
    pub capture: bool,
    /// Every slot's group was empty before the bus stopped.
    pub stages: bool,
}

/// A running stage: its task and where its commands go.
struct Running {
    slot: Slot,
    task: JoinHandle<()>,
    commands: mpsc::UnboundedSender<Command>,
}

/// A running live process.
pub struct Live {
    pipeline: Arc<LivePipeline>,
    backend: InProcess<LiveBlobs>,
    context: StageContext,
    clock: LiveClock,
    activity: Activity,
    stages: Vec<Running>,
    outbox: JoinHandle<()>,
    flushes: mpsc::UnboundedSender<relay::Flush>,
    ticker: Option<JoinHandle<()>>,
    capture: Option<JoinHandle<()>>,
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
            ticking,
            seed,
            capture,
        } = config;
        let flow = FlowSettings::try_from(flow)?;
        let reader = clock.reader();
        surface.clock = Arc::clone(&reader);
        surface.timing = flow.timing;
        let blobs = LiveBlobs::open(&blobs).await?;
        let bus = MpscBus::start(bus).map_err(LiveError::Bus)?;
        let (outbox, outboxed) = Outbox::channel();
        let (relay_events, relayed) = mpsc::unbounded_channel();
        let backend = InProcess::start_with(
            surface,
            Backbone {
                bus: bus.clone(),
                blobs: blobs.clone(),
                outbox,
                events: relayed,
            },
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
            layers: LayerStores::default(),
            publisher: publisher.clone(),
            clock: reader,
            flow,
            seed,
        };
        let mut stages = Stages::default();
        wiring::wire_all(&mut stages, &context, &provenance)?;
        stages.fill(Slot::SurfaceRelay, relay::SurfaceRelay::new(relay_events))?;
        for slot in stages.unfilled() {
            tracing::warn!(slot = slot.name(), "slot unfilled: its layer does not run");
        }
        // Every group subscribes before anything below can publish.
        let mut subscribed = Vec::new();
        for (slot, plug) in stages.into_plugs() {
            let subscription = bus
                .subscribe(&plug.subjects, slot.group(), pipeline.consumer_retry)
                .await
                .map_err(|error| LiveError::Subscribe { slot, error })?;
            subscribed.push((slot, plug, subscription));
        }
        let stages: Vec<Running> = subscribed
            .into_iter()
            .map(|(slot, plug, subscription)| {
                let (commands, received) = mpsc::unbounded_channel();
                let control = Control {
                    retry: pipeline.consumer_retry,
                    commands: received,
                    activity: activity.clone(),
                    slot,
                };
                Running {
                    slot,
                    task: tokio::spawn((plug.run)(subscription, control)),
                    commands,
                }
            })
            .collect();
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
        let capture =
            capture.map(|captured| tokio::spawn(CaptureStage::new(built.ingester()).run(captured)));
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
        })
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
        bus.shutdown().await;
        for running in self.stages {
            join_by(running.slot.name(), running.task, deadline).await;
        }
        self.outbox.abort();
        self.backend.shutdown().await;
        tracing::info!(capture, stages = drained, "live process stopped");
        LiveDrained {
            capture,
            stages: drained,
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
