//! `Live`: the whole detection path and the L8 surface in one process,
//! over one set of in-memory stores. What the UI binary hosts.
//!
//! ```text
//!  proxy capture (optional) ─▶ CaptureStage (L1) ─┐
//!  replay / caller ─▶ pipeline().ingest(exchange, at) ─▶ blobs + ExchangeCaptured
//!                                                  ▼
//!   MpscBus ── group per slot ──▶ L3 ▶ L4 ▶ L5 ▶ L6 classify ▶ L7   (stages; see `stage`)
//!      ▲            │                 └──── write ───▶ LiveStores (crosstalk-memory)
//!      │            ├──▶ evidence feeder ─▶ MemoryEvidence                │ Outbox
//!      │            └──▶ surface relay ─▶ node facts, live feed           ▼
//!      └────────────────────── forward_outbox ◀──────────────────────────┘
//!   Surface<LiveStores> (crosstalk-api's InProcess, over the same stores, bus and blobs)
//! ```
//!
//! [`Live::start`] opens the blob store and starts the bus, builds the
//! surface over them with `InProcess::start_with`, builds the pipeline,
//! fills the slots ([`wiring::wire_all`]), subscribes every slot's group,
//! and only then starts publishing: the outbox forwarder, the stages and
//! the capture stage. Everything time-dependent reads the configured
//! clock, so a corpus or simulation clock replays a dataset through the
//! same path live capture takes.

mod blobs;
mod classify;
mod evidence;
mod relay;
mod stage;
pub mod wiring;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use crosstalk_api::{Backbone, InProcess, InProcessError, InProcessOptions};
use crosstalk_memory::support::Outbox;
use crosstalk_spec::ids::SeededRandom;
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, EventBus};
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
pub use self::evidence::{EvidenceFeeder, NoSpans, SpanSource, SpanSourceError};
pub use self::stage::{
    DERIVED_SUBJECTS, LiveStores, Publisher, Slot, SlotTaken, Stage, StageContext, StageError,
    Stages,
};
use crate::capture::CaptureStage;
use crate::pipeline::{BuildError, Deps, Pipeline, Settings};

/// The pipeline a live process ingests through.
pub type LivePipeline = Pipeline<LiveBlobs, MpscBus>;

/// How a live process is configured.
pub struct LiveConfig {
    /// The surface and its stores; `surface.clock` is the clock every
    /// stage reads.
    pub surface: InProcessOptions,
    pub blobs: BlobConfig,
    pub bus: BusConfig,
    /// Put retries; `consumer_retry` is every slot's group retry policy.
    pub pipeline: Settings,
    /// Seeds the random part of envelope ids.
    pub id_entropy: SeededRandom,
    /// The proxy's capture channel, for a process that serves the L0
    /// listener; `None` to ingest only through [`Live::pipeline`].
    pub capture: Option<mpsc::Receiver<RawExchange>>,
}

/// Why a live process did not start. Nothing it spawned keeps running
/// except what an `InProcess` that started leaves until it is dropped.
#[derive(Debug, thiserror::Error)]
pub enum LiveError {
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

/// A running live process.
pub struct Live {
    pipeline: Arc<LivePipeline>,
    backend: InProcess<LiveBlobs>,
    context: StageContext,
    filled: Vec<Slot>,
    stages: Vec<(Slot, JoinHandle<()>)>,
    outbox: JoinHandle<()>,
    capture: Option<JoinHandle<()>>,
}

impl Live {
    /// Start everything; see the module docs for the order. Needs a tokio
    /// runtime.
    pub async fn start(config: LiveConfig) -> Result<Self, LiveError> {
        let LiveConfig {
            surface,
            blobs,
            bus,
            pipeline,
            id_entropy,
            capture,
        } = config;
        let clock = Arc::clone(&surface.clock);
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
            Deps::stores(blobs, bus.clone(), id_entropy),
            Arc::clone(&clock),
        )
        .await?;
        let publisher = Publisher::new(built.ingester());
        let context = StageContext {
            stores: backend.stores.clone(),
            publisher: publisher.clone(),
            clock,
        };
        let mut stages = Stages::default();
        wiring::wire_all(&mut stages, &context)?;
        stages.fill(Slot::SurfaceRelay, relay::SurfaceRelay::new(relay_events))?;
        for slot in stages.unfilled() {
            tracing::warn!(slot = slot.name(), "slot unfilled: its layer does not run");
        }
        let filled = stages.filled();
        // Every group subscribes before anything below can publish.
        let mut subscribed = Vec::new();
        for (slot, plug) in stages.into_plugs() {
            let subscription = bus
                .subscribe(&plug.subjects, slot.group(), pipeline.consumer_retry)
                .await
                .map_err(|error| LiveError::Subscribe { slot, error })?;
            subscribed.push((slot, plug, subscription));
        }
        let stages = subscribed
            .into_iter()
            .map(|(slot, plug, subscription)| {
                (
                    slot,
                    tokio::spawn((plug.run)(subscription, pipeline.consumer_retry)),
                )
            })
            .collect();
        let outbox = tokio::spawn(relay::forward_outbox(outboxed, publisher));
        let capture =
            capture.map(|captured| tokio::spawn(CaptureStage::new(built.ingester()).run(captured)));
        tracing::info!(
            filled = ?filled.iter().map(|slot| slot.name()).collect::<Vec<_>>(),
            capture = capture.is_some(),
            "live process started"
        );
        Ok(Self {
            pipeline: Arc::new(built),
            backend,
            context,
            filled,
            stages,
            outbox,
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

    /// The stores every stage and the surface share.
    pub fn stores(&self) -> &LiveStores {
        &self.backend.stores
    }

    /// The stores, publisher and clock the stages were built from.
    pub fn context(&self) -> &StageContext {
        &self.context
    }

    /// The slots that run, in slot order.
    pub fn filled(&self) -> &[Slot] {
        &self.filled
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
        let capture = match self.capture {
            Some(task) => join_by("capture stage", task, deadline).await,
            None => true,
        };
        let bus = self.backend.stores.bus.clone();
        let groups: Vec<ConsumerGroup> = self.stages.iter().map(|(slot, _)| slot.group()).collect();
        let drained = wait_for_groups(&bus, &groups, deadline).await;
        bus.shutdown().await;
        for (slot, task) in self.stages {
            join_by(slot.name(), task, deadline).await;
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

/// Wait until every group in `groups` holds nothing at once, until
/// `deadline`.
async fn wait_for_groups(bus: &MpscBus, groups: &[ConsumerGroup], deadline: Instant) -> bool {
    let empty = async {
        'poll: loop {
            for group in groups {
                match bus.depth(group).await {
                    Ok(Some(depth))
                        if depth.ready
                            + depth.delayed
                            + depth.held
                            + depth.exhausted
                            + depth.waiting
                            > 0 =>
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue 'poll;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(group = %group.0, error = ?error, "reading a group's depth failed");
                        return false;
                    }
                }
            }
            return true;
        }
    };
    match tokio::time::timeout_at(deadline, empty).await {
        Ok(drained) => drained,
        Err(_) => {
            tracing::warn!("the stages did not catch up before the deadline");
            false
        }
    }
}
