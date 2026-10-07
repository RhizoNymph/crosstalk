//! `Live` over Postgres: [`PgSet`], and the recovery sequence that starts
//! it ([`Live::start_pg`]). `docs/features/postgres_stores.md`, "Restart
//! semantics end to end", is the design.
//!
//! ```text
//! (the gateway: database reachable, migrations at head, pipeline lock held)
//! recovering_bus     PgBus::recover_held: deliveries a stopped process held go back
//! relaying_outboxes  PgStores::open (L6 outboxes flushed), L3 and L5 relays, L7 drain:
//!                    every staged event published under its stamped id
//! restoring_flow     FlowConsumer::restore: checkpoint, held writes, re-fed accesses
//! rebuilding_nodes   InProcess::host: operators loaded, node facts rebuilt, a new feed
//!                    epoch; interrupted operator actions recorded (AuditIntents)
//! subscribing        every group subscribes (existing groups resume), the stages start,
//!                    the spool's gate opens: what capture spooled drains to the log
//! running
//! ```
//!
//! Everything published during recovery goes through the spool, whose
//! gate is closed until every group subscribed (`crate::spool`), so a
//! group created on this start misses nothing.
//!
//! Stages: L3 [`l3::PgReconstruct`], L4 [`l4::PgProvenanceStage`] (with the
//! extraction step on `PgExtractionLedger`), L5 [`l5`] (the durable flow
//! consumer), L6 [`l6::PgClassify`] (`crosstalk-analysis`'s step), L7
//! [`l7::PgTopology`] (watermark from `PgFrontierSource`), and the surface
//! relay. There is no evidence slot: `PgEvidence` reads the stores. Each
//! stage's subscription is [`pump::Pumped`].

pub mod diagnose;
pub mod l3;
pub mod l4;
pub mod l5;
pub mod l6;
pub mod l7;
pub mod pump;

use std::num::NonZeroU16;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use crosstalk_api::pg::{PgSettings, PgStores};
use crosstalk_api::{CursorSecret, InProcess, PgIds, PgOpen};
use crosstalk_flow::consumer::{DurableInputs, FlowConsumer, FlowDeps, Settings as FlowSettings};
use crosstalk_flow::store::{PgExtractionLedger, PgFlowDurability, PgShardTicks};
use crosstalk_provenance::index::PgFingerprintIndex;
use crosstalk_spec::events::Subject;
use crosstalk_spec::ids::{KeyedHasher, RandomSource};
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus};
use crosstalk_spec::interfaces::l7_topology::EdgeStore;
use crosstalk_store::SerializableRetry;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_topology::outbox::{BusAnnouncer, OutboxIds, drain};
use crosstalk_transport::PgBus;
use tokio::sync::mpsc;

use self::pump::{Pumped, pump};
use super::recovery::{PipelinePhase, RecoveryStep, StatusReporter};
use super::settle::SettleError;
use super::stage::{Activity, Control, Publisher, Slot, StageContext, Stages};
use super::store_set::{LiveStoreSet, Quiet};
use super::{Live, LiveBlobs, LiveConfig, LiveError, Running, Ticking, relay, settle};
use crate::log::consumer::{self as log_consumer, LogStats};
use crate::pipeline::Pipeline;
use crate::spool::{Gate, LiveBus};
use crate::tasks::Tasks;

/// The Postgres set: the Postgres bundle on `PgBus` behind the spool.
#[derive(Debug, Clone, Copy)]
pub struct PgSet;

impl LiveStoreSet for PgSet {
    type Bus = LiveBus;
    type Sub = Pumped;
    type Stores = PgStores<LiveBus, LiveBlobs>;
    type Layers = PgLayers;
    type Quiet = PgQuiet;
    const DEFERRED_ACKS: bool = true;
}

/// What only the Postgres stages and the process read.
#[derive(Debug, Clone)]
pub struct PgLayers {
    pub pool: PgPool,
    /// The bus behind the spool: group stats, prune.
    pub bus: PgBus,
    /// The spool in front of it (also `ctx.stores.bus`).
    pub spool: LiveBus,
    pub ids: PgIds,
    /// The recovery status the process was started with.
    pub status: super::recovery::StatusReader,
}

/// [`Quiet`] over Postgres: group stats, the spool's backlog and the
/// store outboxes.
#[derive(Debug, Clone)]
pub struct PgQuiet {
    bus: PgBus,
    spool: LiveBus,
    pool: PgPool,
}

/// Rows the four store outboxes hold: staged events not yet published. A
/// read-only count across the layers' schemas, for settling only.
const OUTBOX_ROWS: &str = "SELECT (SELECT count(*) FROM reconstruct.outbox) \
     + (SELECT count(*) FROM flow.outbox) \
     + (SELECT count(*) FROM analysis.outbox) \
     + (SELECT count(*) FROM topology.outbox)";

fn in_flight(what: &str, error: impl std::fmt::Debug) -> SettleError {
    SettleError::InFlight {
        reason: format!("{what}: {error:?}"),
    }
}

impl Quiet for PgQuiet {
    async fn busy(&self, slots: &[Slot]) -> Result<bool, SettleError> {
        let groups: Vec<ConsumerGroup> = slots.iter().map(|slot| slot.group()).collect();
        let stats = self
            .bus
            .group_stats()
            .await
            .map_err(|error| in_flight("group stats", error))?;
        if stats
            .iter()
            .any(|stats| stats.pending > 0 && groups.contains(&stats.group))
        {
            return Ok(true);
        }
        if self.spool.stats().records > 0 {
            return Ok(true);
        }
        let (rows,): (i64,) = sqlx::query_as(OUTBOX_ROWS)
            .fetch_one(&self.pool)
            .await
            .map_err(|error| in_flight("outbox rows", error))?;
        Ok(rows > 0)
    }

    async fn flush(&self) -> Result<u64, SettleError> {
        Ok(0)
    }

    async fn wait_group_empty(&self, group: &ConsumerGroup) -> bool {
        loop {
            match self.bus.group_stats().await {
                Ok(stats) => {
                    if stats
                        .iter()
                        .all(|stats| &stats.group != group || stats.pending == 0)
                    {
                        return true;
                    }
                }
                Err(error) => {
                    tracing::warn!(error = ?error, group = %group.0, "reading a group failed");
                    return false;
                }
            }
            tokio::time::sleep(super::POLL.max(Duration::from_millis(10))).await;
        }
    }

    async fn stop(&self) {
        self.bus.shutdown();
    }
}

/// What a Postgres-mode live process starts from besides its config.
/// Clones share every part (a clone kept aside can [`PgParts::diagnose`]
/// a start that does not finish).
#[derive(Clone)]
pub struct PgParts {
    /// The pool, migrations at head (checked by the caller).
    pub pool: PgPool,
    /// The durable bus.
    pub bus: PgBus,
    /// The spool over the gated bus: what every publisher uses.
    pub spool: LiveBus,
    /// Opened once every group subscribed.
    pub gate: Gate,
    /// The capture side's pipeline (already capturing into the spool), when
    /// the process captures; `None` builds one fed only through
    /// [`Live::pipeline`].
    pub pipeline: Option<Arc<Pipeline<LiveBlobs, LiveBus>>>,
    /// The deployment secret the cursor keys derive from.
    pub secret: Arc<KeyedHasher>,
    pub ids: PgIds,
    /// Every store write's serializable retry.
    pub retry: SerializableRetry,
    /// Where each recovery step is reported.
    pub status: StatusReporter,
}

/// Why [`PgParts::open`] did not build the parts.
#[derive(Debug, thiserror::Error)]
pub enum PgPartsError {
    #[error("starting the postgres bus: {0:?}")]
    Bus(crosstalk_transport::StartError),
    #[error("opening the publish spool: {0}")]
    Spool(crosstalk_transport::SpoolError),
}

impl PgParts {
    /// The parts over `pool` (migrated) for a process that captures only
    /// through [`Live::pipeline`] (no proxy): the bus with `bus` settings
    /// on the wall clock ([`bus_clock`]), the spool in `spool` behind its
    /// gate, and a fresh status. For tests and harnesses; `serve` builds
    /// its own (the capture side starts before the database answers).
    pub async fn open(
        pool: PgPool,
        bus: crosstalk_transport::PgBusConfig,
        spool: crosstalk_transport::SpoolConfig,
        secret: Arc<KeyedHasher>,
        ids: PgIds,
    ) -> Result<Self, PgPartsError> {
        let bus = PgBus::new(pool.clone(), bus_clock(), bus).map_err(PgPartsError::Bus)?;
        let gate = Gate::closed();
        let spool = crosstalk_transport::SpoolingBus::open(
            crate::spool::Gated::new(bus.clone(), gate.clone()),
            spool,
        )
        .await
        .map_err(PgPartsError::Spool)?;
        Ok(Self {
            pool,
            bus,
            spool,
            gate,
            pipeline: None,
            secret,
            ids,
            retry: SerializableRetry::default(),
            status: StatusReporter::new(super::recovery::PipelineStatus::waiting()),
        })
    }
}

/// The clock `PgBus` times its delays with (a nacked or timed-out
/// delivery's `available_at`, a recovered hold's backoff): always the wall
/// clock, whatever clock the live process runs on.
///
/// A delayed delivery becomes ready only once the bus clock passes its
/// `available_at`. Under a manual clock that only `Live::settle(until)`
/// moves, a delay taken during a settle would never come due while that
/// settle waits for the groups to empty, and the settle would wait
/// forever (the restart e2e hung this way). `MpscBus` times its retries on
/// tokio time for the same reason. `serve` runs on the wall clock, so for
/// it this changes nothing.
pub fn bus_clock() -> Arc<dyn crosstalk_spec::support::Clock> {
    Arc::new(crosstalk_spec::support::SystemClock)
}

/// The groups whose pending deliveries can still reach a bucket.
const UPSTREAM: [Slot; 5] = [
    Slot::L3Reconstruct,
    Slot::L4Provenance,
    Slot::L5Flow,
    Slot::L6Classify,
    Slot::L7Topology,
];

/// How many extracted batches the L4 stage may have in flight to L5.
const EXTRACTED_IN_FLIGHT: usize = 16;

/// How often the retention tick prunes the bus log.
const RETENTION_EVERY: Duration = Duration::from_secs(60 * 60);

/// How often L7's outbox relay drains without a wake-up.
const TOPOLOGY_RELAY_POLL: Duration = Duration::from_millis(500);

fn recovery_failed(step: RecoveryStep) -> impl FnOnce(&dyn std::fmt::Display) -> LiveError {
    move |error| LiveError::Recovery {
        step: step.label(),
        reason: error.to_string(),
    }
}

impl Live<PgSet> {
    /// Recover and start over Postgres; see the module docs for the order.
    /// The caller has checked the database (reachable, migrations at head)
    /// and holds the pipeline lock. `config.capture` must be `None`: a
    /// capturing process runs its capture stage over `parts.pipeline`.
    pub async fn start_pg(config: LiveConfig, parts: PgParts) -> Result<Self, LiveError> {
        let LiveConfig {
            mut surface,
            clock,
            blobs,
            bus: _,
            pipeline: settings,
            flow,
            provenance,
            extract,
            threading,
            ticking,
            seed,
            capture,
            exchange_log,
        } = config;
        let PgParts {
            pool,
            bus,
            spool,
            gate,
            pipeline,
            secret,
            ids,
            retry,
            status,
        } = parts;
        if capture.is_some() {
            return Err(LiveError::CaptureOutsidePipeline);
        }
        let flow = FlowSettings::try_from(flow)?;
        check_bus(&bus, &flow)?;
        let reader = clock.reader();
        surface.clock = Arc::clone(&reader);
        surface.timing = flow.timing;
        // The ids the surface mints are persisted too: drawn like every
        // other generator's (`surface.ids.unique-across-restart`).
        surface.seed = ids.random(0x5F00).next_u64();
        let blobs = match (&pipeline, blobs) {
            (Some(pipeline), _) => pipeline.blobs().clone(),
            (None, blobs) => LiveBlobs::open(&blobs).await?,
        };

        status.phase(PipelinePhase::Recovering(RecoveryStep::RecoveringBus));
        let recovered = bus
            .recover_held()
            .await
            .map_err(|error| recovery_failed(RecoveryStep::RecoveringBus)(&format!("{error:?}")))?;

        status.phase(PipelinePhase::Recovering(RecoveryStep::RelayingOutboxes));
        let (stores, topology_relay) = PgStores::open(PgOpen {
            pool: pool.clone(),
            bus: spool.clone(),
            dead_letters: bus.dead_letters(),
            blobs: blobs.clone(),
            clock: Arc::clone(&reader),
            secret: Arc::clone(&secret),
            ids,
            settings: PgSettings {
                bucket_width: surface.bucket_width,
                timing: surface.timing,
                retention: surface.retention,
                lineage_floor: surface.lineage_floor,
                embedding_model: surface.embedding_model.clone(),
                default_remap_threshold: surface.surface.default_remap_threshold,
                projection_lease: surface.projection_lease,
                frame_retention: surface.surface.frame_retention.as_duration(),
                sinks: surface.sinks.clone(),
                configure_sinks: true,
                threading,
                retry,
            },
        })
        .await
        .map_err(LiveError::Stores)?;
        let relayed_agents = stores
            .agents
            .flush_outbox()
            .await
            .map_err(|error| recovery_failed(RecoveryStep::RelayingOutboxes)(&error))?;
        let relayed_flow = stores
            .channels
            .relay()
            .relay()
            .await
            .map_err(|error| recovery_failed(RecoveryStep::RelayingOutboxes)(&error))?;
        let announcer = BusAnnouncer::new(spool.clone());
        let mut outbox_ids = OutboxIds::new(Arc::clone(&reader), ids.random(0x70B0));
        let relayed_topology = drain(&pool, &mut outbox_ids, &announcer)
            .await
            .map_err(|error| recovery_failed(RecoveryStep::RelayingOutboxes)(&error))?;
        let outbox_relayed =
            u64::try_from(relayed_agents + relayed_flow).unwrap_or(u64::MAX) + relayed_topology;

        status.phase(PipelinePhase::Recovering(RecoveryStep::RestoringFlow));
        let mut consumer = FlowConsumer::with_durability(
            flow,
            FlowDeps {
                registry: stores.channels.clone(),
                transmissions: stores.transmissions.clone(),
                agents: stores.agents.clone(),
                bus: spool.clone(),
                clock: Arc::clone(&reader),
            },
            PgFlowDurability::new(pool.clone(), retry),
        );
        let restored = consumer.restore().await.map_err(LiveError::Restore)?;
        status.update(|status| {
            status.report.deliveries_redelivered = recovered.redelivered;
            status.report.outbox_relayed = outbox_relayed;
            status.report.flow_checkpoint_micros = restored.ticked_through.map(|at| at.as_micros());
            status.report.accesses_refed =
                u64::try_from(restored.accesses_refed + restored.tool_calls_refed)
                    .unwrap_or(u64::MAX);
        });
        tracing::info!(
            redelivered = recovered.redelivered,
            dead_lettered = recovered.dead_lettered,
            outbox_relayed,
            checkpoint = restored.checkpoint,
            held_writes = restored.held_writes,
            accesses_refed = restored.accesses_refed,
            tool_calls_refed = restored.tool_calls_refed,
            "postgres recovery: bus, outboxes and flow restored"
        );

        status.phase(PipelinePhase::Recovering(RecoveryStep::RebuildingNodes));
        let (relay_events, relayed) = mpsc::unbounded_channel();
        let backend = InProcess::host(
            stores,
            surface,
            relayed,
            CursorSecret::Derived(&secret),
            "gateway access config",
        )
        .await?;
        let interrupted = backend
            .surface
            .recover_interrupted()
            .await
            .map_err(|error| {
                recovery_failed(RecoveryStep::RebuildingNodes)(&format!("{error:?}"))
            })?;
        let watermark = backend.stores.edges.watermark().await.map_err(|error| {
            recovery_failed(RecoveryStep::RebuildingNodes)(&format!("{error:?}"))
        })?;
        tracing::info!(
            interrupted = interrupted.len(),
            watermark = watermark.at().as_micros(),
            "postgres recovery: node facts rebuilt, persisted watermark read"
        );

        status.phase(PipelinePhase::Recovering(RecoveryStep::Subscribing));
        let pipeline = match pipeline {
            Some(pipeline) => pipeline,
            None => Arc::new(
                Pipeline::build(
                    settings,
                    crate::pipeline::Deps {
                        // The filesystem and memory blob stores never drop a
                        // body (the spec's `BlobStore` has no delete).
                        bodies: crate::pipeline::Bodies::SkipStored,
                        ..crate::pipeline::Deps::stores(
                            blobs.clone(),
                            spool.clone(),
                            ids.random(0xE7E7),
                        )
                    },
                    Arc::clone(&reader),
                )
                .await?,
            ),
        };
        let activity = Activity::default();
        let context = StageContext {
            stores: backend.stores.clone(),
            layers: PgLayers {
                pool: pool.clone(),
                bus: bus.clone(),
                spool: spool.clone(),
                ids,
                status: status.reader(),
            },
            publisher: Publisher::new(pipeline.ingester()),
            clock: Arc::clone(&reader),
            flow,
            seed,
            watermark: Arc::new(AtomicU64::new(watermark.at().as_micros())),
        };
        let (durable, extracted) = DurableInputs::channel(EXTRACTED_IN_FLIGHT);
        let mut stages: Stages<Pumped> = Stages::empty();
        stages.fill(Slot::L3Reconstruct, l3::PgReconstruct::new(&context, ids))?;
        stages.fill(
            Slot::L4Provenance,
            l4::PgProvenanceStage::new(
                &provenance,
                &extract,
                l4::PgProvenanceParts {
                    provenance: context.stores.provenance.clone(),
                    index: PgFingerprintIndex::new(pool.clone(), provenance.index().clone()),
                    ledger: PgExtractionLedger::new(pool.clone()),
                    blobs: blobs.clone(),
                    bus: spool.clone(),
                    flow: durable,
                    content_retention: flow.content_retention.get(),
                },
            ),
        )?;
        l5::fill(
            &mut stages,
            consumer,
            extracted,
            ticking == Ticking::OnSettle,
        )?;
        stages.fill(
            Slot::L6Classify,
            l6::PgClassify::new(
                context.stores.catalog.clone(),
                context.stores.transmissions.clone(),
                spool.clone(),
            ),
        )?;
        let shards = u16::try_from(flow.shards.get())
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or(LiveError::TooManyShards(flow.shards.get()))?;
        stages.fill(
            Slot::L7Topology,
            l7::PgTopology::new(
                context.stores.edges.clone(),
                spool.clone(),
                crate::live::frontier::PgFrontierSource::new(
                    bus.clone(),
                    pool.clone(),
                    PgShardTicks::new(pool.clone()),
                    shards,
                    UPSTREAM.iter().map(|slot| slot.group()).collect(),
                    spool.clone(),
                ),
                Arc::clone(&context.watermark),
            ),
        )?;
        stages.fill(Slot::SurfaceRelay, relay::SurfaceRelay::new(relay_events))?;

        // Every group subscribes before the gate opens.
        let log_subscription = match &exchange_log {
            Some(_) => Some(pump(
                bus.subscribe(
                    &[Subject::ExchangeCaptured],
                    log_consumer::group(),
                    settings.consumer_retry,
                )
                .await
                .map_err(LiveError::LogSubscribe)?,
            )),
            None => None,
        };
        let mut subscribed = Vec::new();
        for (slot, plug) in stages.into_plugs() {
            let subscription = bus
                .subscribe(&plug.subjects, slot.group(), settings.consumer_retry)
                .await
                .map_err(|error| LiveError::Subscribe { slot, error })?;
            subscribed.push((slot, plug, pump(subscription)));
        }
        let mut stage_tasks = Tasks::new();
        let running: Vec<Running> = subscribed
            .into_iter()
            .map(|(slot, plug, subscription)| {
                let (commands, received) = mpsc::unbounded_channel();
                let own = activity.stage();
                let control = Control {
                    retry: settings.consumer_retry,
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
        let topology = tokio::spawn(topology_relay.run(announcer, outbox_ids, TOPOLOGY_RELAY_POLL));
        let retention = tokio::spawn(prune_periodically(bus.clone(), Arc::clone(&reader)));
        let ticker = match ticking {
            Ticking::Periodic => Some(tokio::spawn(settle::tick_periodically(
                settle::commands_of(&running),
                clock.clone(),
                flow.tick_every,
            ))),
            Ticking::OnSettle => None,
        };
        gate.open();
        status.phase(PipelinePhase::Running);
        tracing::info!(
            stages = ?running.iter().map(|running| running.slot.name()).collect::<Vec<_>>(),
            ticking = ?ticking,
            "live process started on postgres"
        );
        Ok(Self {
            pipeline,
            backend,
            context,
            clock,
            activity,
            stages: running,
            quiet: PgQuiet { bus, spool, pool },
            publishers: vec![topology, retention],
            ticker,
            capture: None,
            exchange_log,
            log_stats,
            tasks,
            stage_tasks,
        })
    }
}

impl Live<PgSet> {
    /// What the process is waiting on now (see [`diagnose`]): for a
    /// settle or shutdown that does not finish.
    pub async fn diagnose(&self) -> diagnose::PgDiagnosis {
        let layers = &self.context.layers;
        let groups: Vec<ConsumerGroup> = UPSTREAM.iter().map(|slot| slot.group()).collect();
        let shards = u16::try_from(self.context.flow.shards.get())
            .ok()
            .and_then(NonZeroU16::new)
            .unwrap_or(NonZeroU16::MIN);
        let watermark = self
            .backend
            .stores
            .edges
            .watermark()
            .await
            .map(|watermark| watermark.at())
            .map_err(|error| format!("{error:?}"));
        diagnose::diagnose(diagnose::DiagnoseFrom {
            pool: &layers.pool,
            bus: &layers.bus,
            spool: Some(&layers.spool),
            groups: &groups,
            shards,
            status: Some(&layers.status),
            watermark: Some(watermark),
        })
        .await
    }
}

impl PgParts {
    /// What the parts' database and spool hold now, before (or without) a
    /// live process: for a recovery that does not finish.
    pub async fn diagnose(&self, shards: NonZeroU16) -> diagnose::PgDiagnosis {
        let groups: Vec<ConsumerGroup> = UPSTREAM.iter().map(|slot| slot.group()).collect();
        let status = self.status.reader();
        diagnose::diagnose(diagnose::DiagnoseFrom {
            pool: &self.pool,
            bus: &self.bus,
            spool: Some(&self.spool),
            groups: &groups,
            shards,
            status: Some(&status),
            watermark: None,
        })
        .await
    }
}

/// The bus settings the durable flow consumer depends on: a delivery is
/// held until a checkpoint covers it, so the ack timeout must outlast the
/// checkpoint interval, and the group must admit the deliveries one
/// checkpoint waits for.
fn check_bus(bus: &PgBus, flow: &FlowSettings) -> Result<(), LiveError> {
    let config = bus.config();
    if config.ack_timeout.get() <= flow.checkpoint_every {
        return Err(LiveError::AckTimeout {
            ack_timeout_ms: u64::try_from(config.ack_timeout.get().as_millis()).unwrap_or(u64::MAX),
            checkpoint_ms: u64::try_from(flow.checkpoint_every.as_millis()).unwrap_or(u64::MAX),
        });
    }
    if flow.max_unacked.get() > config.group_capacity.get() {
        return Err(LiveError::UnackedAboveCapacity {
            unacked: flow.max_unacked.get(),
            capacity: config.group_capacity.get(),
        });
    }
    Ok(())
}

/// Prune the bus log every [`RETENTION_EVERY`]: entries every group is
/// done with and older than the configured retention (decision Q6).
async fn prune_periodically(bus: PgBus, clock: Arc<dyn crosstalk_spec::support::Clock>) {
    let keep = bus.config().retention.get();
    let mut ticker = tokio::time::interval(RETENTION_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match bus.prune(clock.now(), keep).await {
            Ok(pruned) => tracing::debug!(pruned, "bus log pruned"),
            Err(error) => tracing::warn!(error = ?error, "bus log prune failed; retried next tick"),
        }
    }
}
