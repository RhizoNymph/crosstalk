//! The restart simulation's world: a seeded scenario of extracted inputs,
//! bus deliveries and ticks, and a driver that runs it through a durable
//! consumer over the reference stores, crashing and restoring it where the
//! plan says.
//!
//! The driver plays the bus and the extraction step:
//!
//! - a delivery stays unacked until a checkpoint covers it; a crash
//!   redelivers every unacked delivery to the restored consumer;
//! - a batch of extracted inputs is retried (as L4 retries its delta)
//!   until the consumer confirmed it durable;
//! - an outage makes the bus refuse publishes and the durability port
//!   refuse writes, so steps wait in the queue (a store write done, its
//!   event not published; a held write released, its row not dropped)
//!   when a crash comes.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_memory::flow::MemoryVerdicts;
use crosstalk_memory::reconstruct::MemoryAgents;
use crosstalk_sim::{Seed, SimRng};
use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::flow::transmission::Transmission;
use crosstalk_spec::derived::provenance::matching::Carrier;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AccessId, AgentId, ChannelId, EventId, SpanId, TransmissionId};
use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup, EventBus, RetryPolicy};
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::{
    AgentLifecycle, AgentOrigin, NewAgent,
};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::observed::agent::IdentityEvidence;
use crosstalk_spec::observed::message::{ToolCallId, ToolName};
use crosstalk_spec::support::{Clock, NonEmpty, Timestamp};
use crosstalk_testkit::build::exchange::ExchangeBuilder;
use crosstalk_testkit::time::{T0, after};

use crate::consumer::tests::harness::{
    Registry, Silent, Stores, found_in, matched, read, settings, wiki_page, write,
};
use crate::consumer::{
    Checkpoint, DurabilityError, Extracted, FlowConsumer, FlowDeps, FlowDurability,
    MemoryDurability, Observed, Recovered, ToolCalled, WriteCall,
};
use crate::correlate::pairing::WriteOutcome;
use crate::correlate::tests::fixtures::Scene;

/// One thing the scenario does.
#[derive(Debug, Clone)]
pub(super) enum Item {
    /// Extracted inputs of one delta, handed over together.
    Batch(Vec<Extracted>),
    /// A bus delivery to the flow group.
    Deliver(Box<BusEvent>),
    Tick(Timestamp),
    /// The consumer's periodic checkpoint, then the acks it covers.
    Checkpoint,
}

/// A scenario: its agents and what happens, in order.
#[derive(Debug, Clone)]
pub(super) struct Plan {
    agents: Vec<(AgentId, Option<AgentId>)>,
    pub(super) items: Vec<Item>,
    end: Timestamp,
}

fn rng_below(rng: &mut SimRng, bound: u64) -> u64 {
    rng.below(NonZeroU64::new(bound).unwrap_or(NonZeroU64::MIN))
}

fn pick<T: Copy>(rng: &mut SimRng, items: &[T]) -> Option<T> {
    rng.pick(items).copied()
}

/// The scenario of `seed`: agents writing and reading three wiki pages,
/// some writes held for their result, content matches for reads of
/// written spans (late, duplicated), tool-result, user-turn and
/// delegation matches, ticks every five seconds, and, when `checkpoints`,
/// a checkpoint every few items.
pub(super) fn scenario(seed: u64, checkpoints: bool) -> Plan {
    let mut rng = SimRng::new(Seed::new(seed));
    let mut scene = Scene::new(u32::try_from(seed % u64::from(u32::MAX)).unwrap_or(1));
    let roots: Vec<AgentId> = (0..4).map(|_| scene.agent()).collect();
    let child = scene.agent();
    let mut agents: Vec<(AgentId, Option<AgentId>)> = roots.iter().map(|a| (*a, None)).collect();
    agents.push((child, Some(roots[0])));
    let all: Vec<AgentId> = agents.iter().map(|(agent, _)| *agent).collect();
    let pages: Vec<Locator> = ["Alpha", "Beta", "Gamma"].map(wiki_page).to_vec();
    let mut timed: Vec<(Timestamp, Item)> = Vec::new();
    let mut written: Vec<(usize, AgentId, SpanId)> = Vec::new();
    let mut delivered: Vec<BusEvent> = Vec::new();
    let mut t = after(T0, Duration::from_secs(1));
    let later = |rng: &mut SimRng, at: Timestamp, max: u64| {
        after(at, Duration::from_secs(rng_below(rng, max)))
    };
    for _ in 0..36 {
        t = after(t, Duration::from_secs(1 + rng_below(&mut rng, 25)));
        match rng_below(&mut rng, 9) {
            0..=2 => {
                let (Some(agent), Some(page)) = (pick(&mut rng, &all), rng.index(pages.len()))
                else {
                    continue;
                };
                let span = scene.span();
                let edit = write(&mut scene, agent, &pages[page], t, vec![span]);
                written.push((page, agent, span));
                match rng_below(&mut rng, 3) {
                    0 => timed.push((
                        t,
                        Item::Batch(vec![Extracted::Write {
                            write: edit,
                            outcome: Some(WriteOutcome::Delivered),
                        }]),
                    )),
                    held => {
                        let id = edit.id;
                        timed.push((
                            t,
                            Item::Batch(vec![Extracted::Write {
                                write: edit,
                                outcome: None,
                            }]),
                        ));
                        // A result for one of the two held kinds; the
                        // other settles as `Unknown` on a tick.
                        if held == 1 {
                            let outcome = match rng_below(&mut rng, 4) {
                                0 => WriteOutcome::Rejected,
                                _ => WriteOutcome::Delivered,
                            };
                            timed.push((
                                later(&mut rng, t, 90),
                                Item::Batch(vec![Extracted::WriteResult {
                                    access: id,
                                    outcome,
                                }]),
                            ));
                        }
                    }
                }
            }
            3..=5 => {
                let (Some(reader), Some(page)) = (pick(&mut rng, &all), rng.index(pages.len()))
                else {
                    continue;
                };
                let fetch = read(&mut scene, reader, &pages[page], t);
                let matches: Vec<(AgentId, SpanId)> = written
                    .iter()
                    .filter(|(on, writer, _)| *on == page && *writer != reader)
                    .map(|(_, writer, span)| (*writer, *span))
                    .collect();
                for (writer, span) in matches {
                    if rng_below(&mut rng, 10) < 6 {
                        let content = found_in(&mut scene, &fetch, writer, span);
                        let event = matched(&content);
                        delivered.push(event.clone());
                        timed.push((later(&mut rng, t, 90), Item::Deliver(Box::new(event))));
                    }
                }
                timed.push((t, Item::Batch(vec![Extracted::Read(fetch)])));
            }
            6 | 7 => {
                let (Some(from), Some(to)) = (pick(&mut rng, &all), pick(&mut rng, &all)) else {
                    continue;
                };
                if from == to {
                    continue;
                }
                let exchange = scene.exchange();
                let mut captured = ExchangeBuilder::new(&mut scene.ids).started_at(t).build();
                captured.meta.id = exchange;
                timed.push((
                    t,
                    Item::Deliver(Box::new(BusEvent::Ingest(IngestEvent::ExchangeCaptured(
                        Box::new(captured),
                    )))),
                ));
                let span = scene.span();
                let carrier = if rng_below(&mut rng, 2) == 0 {
                    let call = ToolCallId(format!("toolu_{}", exchange.ulid_text()));
                    timed.push((
                        t,
                        Item::Batch(vec![Extracted::ToolCall {
                            agent: to,
                            call: call.clone(),
                            name: ToolName("Bash".to_owned()),
                            at: t,
                        }]),
                    ));
                    Carrier::ToolResult(call)
                } else {
                    Carrier::UserTurn
                };
                let content = scene.found(from, to, exchange, span, carrier);
                let event = matched(&content);
                delivered.push(event.clone());
                timed.push((later(&mut rng, t, 40), Item::Deliver(Box::new(event))));
            }
            _ => {
                // A redelivery of an earlier delivery.
                if let Some(index) = rng.index(delivered.len()) {
                    timed.push((t, Item::Deliver(Box::new(delivered[index].clone()))));
                }
            }
        }
    }
    let end = after(t, Duration::from_secs(120));
    let mut tick = T0;
    while tick <= end {
        timed.push((tick, Item::Tick(tick)));
        tick = after(tick, Duration::from_secs(5));
    }
    // Stable: at one time, inputs before the tick that follows them.
    timed.sort_by_key(|(at, item)| (*at, matches!(item, Item::Tick(_))));
    let mut items = Vec::with_capacity(timed.len() * 2);
    let mut until_checkpoint = 3 + rng_below(&mut rng, 6);
    for (_, item) in timed {
        items.push(item);
        if checkpoints {
            until_checkpoint -= 1;
            if until_checkpoint == 0 {
                items.push(Item::Checkpoint);
                until_checkpoint = 3 + rng_below(&mut rng, 6);
            }
        }
    }
    Plan { agents, items, end }
}

/// Where the simulated process fails.
#[derive(Debug, Clone, Default)]
pub(super) struct Faults {
    /// The item indices before which the process crashes.
    pub(super) crashes: BTreeSet<usize>,
    /// Outage windows: `(from, to)` item indices.
    pub(super) outages: Vec<(usize, usize)>,
}

impl Faults {
    /// Seeded crashes and outages over `items` items.
    pub(super) fn seeded(seed: u64, items: usize) -> Self {
        let mut rng = SimRng::new(Seed::new(seed ^ 0x00C0_FFEE));
        let len = u64::try_from(items).unwrap_or(1).max(1);
        let mut faults = Faults::default();
        for _ in 0..(1 + rng_below(&mut rng, 4)) {
            let at = usize::try_from(rng_below(&mut rng, len)).unwrap_or(0);
            faults.crashes.insert(at);
        }
        for _ in 0..rng_below(&mut rng, 3) {
            let from = usize::try_from(rng_below(&mut rng, len)).unwrap_or(0);
            let to = from + usize::try_from(1 + rng_below(&mut rng, 12)).unwrap_or(1);
            faults.outages.push((from, to));
        }
        faults
    }
}

/// Whether the store and bus are down.
#[derive(Debug, Clone, Default)]
struct Outage(Arc<AtomicBool>);

impl Outage {
    fn set(&self, down: bool) {
        self.0.store(down, Ordering::SeqCst);
    }

    fn down(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// The bus log, holding each envelope id once (as `PgBus` does), behind
/// the outage.
#[derive(Debug, Clone, Default)]
pub(super) struct Log {
    envelopes: Arc<Mutex<BTreeMap<EventId, Envelope>>>,
    /// Every publish that reached the log, repeats included.
    publishes: Arc<Mutex<Vec<EventId>>>,
    outage: Outage,
}

impl Log {
    pub(super) fn envelopes(&self) -> BTreeMap<EventId, Envelope> {
        self.envelopes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn publishes(&self) -> Vec<EventId> {
        self.publishes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl EventBus for Log {
    type Subscription = Silent;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        if self.outage.down() {
            return Err(BusError::Disconnected);
        }
        self.publishes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(envelope.id);
        self.envelopes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(envelope.id)
            .or_insert(envelope);
        Ok(())
    }

    async fn subscribe(
        &self,
        _subjects: &[Subject],
        _group: ConsumerGroup,
        _retry: RetryPolicy,
    ) -> Result<Silent, BusError> {
        Ok(Silent)
    }
}

/// The memory durability behind the outage.
#[derive(Debug, Clone, Default)]
pub(super) struct Flaky {
    pub(super) inner: MemoryDurability,
    outage: Outage,
}

impl Flaky {
    fn up(&self) -> Result<(), DurabilityError> {
        match self.outage.down() {
            true => Err(DurabilityError::Unavailable {
                reason: "simulated outage".to_owned(),
            }),
            false => Ok(()),
        }
    }
}

impl FlowDurability for Flaky {
    fn survives_restart(&self) -> bool {
        true
    }

    async fn hold(
        &self,
        write: &Observed<WriteCall>,
        settles_at: Timestamp,
    ) -> Result<(), DurabilityError> {
        self.up()?;
        self.inner.hold(write, settles_at).await
    }

    async fn release(&self, access: AccessId) -> Result<(), DurabilityError> {
        self.up()?;
        self.inner.release(access).await
    }

    async fn access_recorded(
        &self,
        access: &Access,
        locator: &Locator,
        channel: Option<ChannelId>,
    ) -> Result<(), DurabilityError> {
        // The registry numbers the access as it records it; only the
        // resolution, written after, can fail.
        if self.outage.down() {
            self.inner.record_unresolved(access, locator);
        }
        self.up()?;
        self.inner.access_recorded(access, locator, channel).await
    }

    async fn tool_called(&self, call: &ToolCalled) -> Result<(), DurabilityError> {
        self.up()?;
        self.inner.tool_called(call).await
    }

    async fn save(
        &self,
        checkpoint: &Checkpoint,
        taken_at: Timestamp,
    ) -> Result<(), DurabilityError> {
        self.up()?;
        self.inner.save(checkpoint, taken_at).await
    }

    async fn load(&self) -> Result<Recovered, DurabilityError> {
        self.inner.load().await
    }
}

pub(super) type Consumer = FlowConsumer<Registry, MemoryVerdicts, MemoryAgents, Log, Flaky>;

/// A clock that never moves: envelope and checkpoint times only.
#[derive(Debug)]
struct Fixed(Timestamp);

impl Clock for Fixed {
    fn now(&self) -> Timestamp {
        self.0
    }
}

/// One process's stores, bus log and durability: what outlives a crash.
pub(super) struct World {
    pub(super) stores: Stores,
    pub(super) log: Log,
    pub(super) durability: Flaky,
    outage: Outage,
}

impl World {
    pub(super) async fn new(plan: &Plan) -> Self {
        let stores = Stores::new();
        for (agent, parent) in &plan.agents {
            let created = stores
                .agents
                .clone()
                .create(NewAgent {
                    id: *agent,
                    evidence: NonEmpty::new(IdentityEvidence::StableCredential(
                        crosstalk_testkit::ids::Ids::seeded(
                            u32::try_from(agent.as_ulid() % 1_000_000).unwrap_or(7),
                        )
                        .credential(),
                    )),
                    parent: *parent,
                    origin: AgentOrigin::Traffic { first_seen: T0 },
                    label: None,
                })
                .await;
            assert_eq!(created, Ok(()), "agent {agent:?}");
        }
        let outage = Outage::default();
        Self {
            stores,
            log: Log {
                outage: outage.clone(),
                ..Log::default()
            },
            durability: Flaky {
                outage: outage.clone(),
                ..Flaky::default()
            },
            outage,
        }
    }

    /// Take the store and bus down, or bring them back.
    pub(super) fn set_outage(&self, down: bool) {
        self.outage.set(down);
    }

    /// A fresh consumer process over this world's stores, restored.
    pub(super) async fn start(&self) -> Consumer {
        let mut consumer = FlowConsumer::with_durability(
            settings(3),
            FlowDeps {
                registry: self.stores.registry.clone(),
                transmissions: self.stores.transmissions.clone(),
                agents: self.stores.agents.clone(),
                bus: self.log.clone(),
                clock: Arc::new(Fixed(T0)),
            },
            self.durability.clone(),
        );
        match consumer.restore().await {
            Ok(_) => consumer,
            Err(error) => panic!("restore: {error}"),
        }
    }
}

/// What a run decided, read through the stores and the bus log.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Decided {
    /// Every transmission the store holds, by id.
    pub(super) transmissions: BTreeMap<TransmissionId, Transmission>,
    /// Every channel transmission as the registry recorded it.
    pub(super) recorded: BTreeMap<TransmissionId, Transmission>,
}

/// Every transmission id a run's decisions name.
async fn decided(world: &World) -> Decided {
    let mut ids: BTreeSet<TransmissionId> = BTreeSet::new();
    let mut recorded = BTreeMap::new();
    for channel in world.stores.channels().await {
        for transmission in world.stores.transmissions_of(channel.channel().id).await {
            ids.insert(transmission.id);
            recorded.insert(transmission.id, transmission);
        }
    }
    for envelope in world.log.envelopes().values() {
        if let BusEvent::Detect(
            DetectEvent::TransmissionConfirmed { transmission, .. }
            | DetectEvent::TransmissionSuspected { transmission, .. },
        ) = &envelope.event
        {
            ids.insert(*transmission);
        }
    }
    let mut transmissions = BTreeMap::new();
    for id in ids {
        match world.stores.transmissions.transmission(id).await {
            Ok(Some(transmission)) => {
                transmissions.insert(id, transmission);
            }
            other => panic!("transmission {id:?}: {other:?}"),
        }
    }
    Decided {
        transmissions,
        recorded,
    }
}

/// The bus and the extraction step around one consumer process.
struct Driver {
    consumer: Consumer,
    /// Deliveries taken and not acked, in delivery order.
    unacked: Vec<BusEvent>,
    /// Batches not yet confirmed durable, in order.
    unconfirmed: Vec<Vec<Extracted>>,
}

impl Driver {
    async fn batch(&mut self, inputs: Vec<Extracted>) {
        if self.consumer.handle_batch(inputs.clone()).await.is_err() {
            self.unconfirmed.push(inputs);
        }
    }

    /// Retry every batch not confirmed, as L4 retries its deltas.
    async fn retry(&mut self) {
        for inputs in std::mem::take(&mut self.unconfirmed) {
            self.batch(inputs).await;
        }
    }

    async fn checkpoint(&mut self, world: &World) {
        if self.consumer.checkpoint().await.is_ok() {
            self.unacked.clear();
            // flow.checkpoint.ticks-with-state: no tick record names a
            // tick later than the stored checkpoint of its shard.
            let stored = world.durability.inner.checkpoint();
            for (shard, tick) in world.durability.inner.ticks() {
                let snapshot = stored
                    .as_ref()
                    .and_then(|stored| stored.shards.iter().find(|row| row.shard == shard))
                    .and_then(|row| row.ticked_through);
                assert!(
                    snapshot.is_some_and(|at| tick <= at),
                    "shard {shard}: tick record {tick:?} ahead of its checkpoint {snapshot:?}"
                );
            }
        }
    }

    /// The process dies: everything in memory is lost. A new one restores
    /// and the bus redelivers what was not acked; L4 retries its deltas.
    async fn crash(&mut self, world: &World) {
        world.outage.set(false);
        self.consumer = world.start().await;
        for event in self.unacked.clone() {
            self.consumer.handle_event(&event).await;
        }
        self.retry().await;
    }
}

/// Run `plan` in `world` under `faults`, then settle: tick past every
/// window and suspicion, checkpointing.
pub(super) async fn run(plan: &Plan, faults: &Faults) -> (World, Decided) {
    let world = World::new(plan).await;
    let mut driver = Driver {
        consumer: world.start().await,
        unacked: Vec::new(),
        unconfirmed: Vec::new(),
    };
    for (index, item) in plan.items.iter().enumerate() {
        if faults.crashes.contains(&index) {
            driver.crash(&world).await;
        }
        for (from, to) in &faults.outages {
            if *from == index {
                world.outage.set(true);
            }
            if *to == index {
                world.outage.set(false);
                driver.retry().await;
            }
        }
        match item {
            Item::Batch(inputs) => driver.batch(inputs.clone()).await,
            Item::Deliver(event) => {
                driver.consumer.handle_event(event).await;
                driver.unacked.push((**event).clone());
            }
            Item::Tick(now) => driver.consumer.tick(*now).await,
            Item::Checkpoint => driver.checkpoint(&world).await,
        }
    }
    world.outage.set(false);
    driver.retry().await;
    for minutes in [1, 10, 30, 60, 120, 24 * 60] {
        driver
            .consumer
            .tick(after(plan.end, Duration::from_secs(minutes * 60)))
            .await;
        driver.retry().await;
        driver.checkpoint(&world).await;
    }
    assert_eq!(driver.consumer.backlog(), 0, "steps left in the queue");
    let decided = decided(&world).await;
    (world, decided)
}
