//! Simulation evidence of the gateway's Postgres-mode composition
//! (`crosstalk_gateway::dst::<name>`), on paused time with seeded choices.
//!
//! - `frontier_covers_spooled_envelopes`: `topology.frontier.covers-spool`
//!   (INV-1217) and `topology.frontier.covers-pending` over the publish
//!   spool: the frontier `PgFrontierSource` computes, from the spool's
//!   oldest record and the groups' backlog, never runs past an envelope
//!   still spooled or pending, through seeded outages and drains.
//! - `classifier_redelivery_republishes_the_same_envelope_ids`:
//!   `transport.consumer.derived-envelope-ids` (INV-1202) for the L6 step
//!   the gateway runs in Postgres mode (`crosstalk-analysis`'s
//!   `Classifier`): every redelivery of a confirmation returns the same
//!   envelope, id derived from the delivery.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crosstalk_analysis::classify::{CLASSIFIED_LABEL, Classifier};
use crosstalk_api::InProcess;
use crosstalk_flow::consumer::FlowConfig;
use crosstalk_memory::support::ManualClock;
use crosstalk_sim::{CheckFailed, Probability, SimCtx};
use crosstalk_spec::derived::flow::transmission::Route;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, EventId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeliveryId, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::build::TransmissionBuilder;
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;
use crosstalk_transport::{
    BusConfig, DrainTarget, GroupStats, MpscBus, MpscSubscription, NonZeroDuration, SpoolConfig,
    SpoolingBus,
};

use crate::live::frontier::combine;
use crate::live::{LiveClock, LiveConfig};

crosstalk_sim::sim_test! {
    /// `topology.frontier.covers-spool` (INV-1217): see the module docs.
    fn frontier_covers_spooled_envelopes(ctx) {
        frontier_covers_spool(ctx).await
    }
}

crosstalk_sim::sim_test! {
    /// `transport.consumer.derived-envelope-ids` (INV-1202), the gateway's
    /// L6 step: see the module docs.
    fn classifier_redelivery_republishes_the_same_envelope_ids(ctx) {
        classifier_redelivery(ctx).await
    }
}

fn failed(what: &str, error: impl std::fmt::Debug) -> CheckFailed {
    CheckFailed::new(format!("{what}: {error:?}"))
}

/// An inner bus that goes down on command, recording what it took.
#[derive(Clone)]
struct Flaky {
    bus: MpscBus,
    down: Arc<Mutex<bool>>,
    /// Every envelope the inner bus took, by id, with its `at`.
    taken: Arc<Mutex<BTreeMap<EventId, Timestamp>>>,
}

impl Flaky {
    fn is_down(&self) -> bool {
        *self.down.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_down(&self, down: bool) {
        *self.down.lock().unwrap_or_else(PoisonError::into_inner) = down;
    }

    fn taken(&self) -> BTreeMap<EventId, Timestamp> {
        self.taken
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn take(&self, envelopes: Vec<Envelope>) -> Result<(), BusError> {
        if self.is_down() {
            return Err(BusError::Disconnected);
        }
        for envelope in envelopes {
            let (id, at) = (envelope.id, envelope.at);
            let known = self
                .taken
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(&id);
            // Idempotent on ids, as PgBus is.
            if !known {
                self.bus.publish(envelope).await?;
                self.taken
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id, at);
            }
        }
        Ok(())
    }
}

impl EventBus for Flaky {
    type Subscription = MpscSubscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        self.take(vec![envelope]).await
    }

    async fn subscribe(
        &self,
        subjects: &[Subject],
        group: ConsumerGroup,
        retry: RetryPolicy,
    ) -> Result<MpscSubscription, BusError> {
        self.bus.subscribe(subjects, group, retry).await
    }
}

impl DrainTarget for Flaky {
    async fn probe(&self) -> Result<(), BusError> {
        match self.is_down() {
            true => Err(BusError::Disconnected),
            false => Ok(()),
        }
    }

    async fn publish_batch(&self, envelopes: Vec<Envelope>) -> Result<(), BusError> {
        self.take(envelopes).await
    }
}

async fn frontier_covers_spool(ctx: SimCtx) -> Result<(), CheckFailed> {
    let mut rng = ctx.rng();
    let dir = tempfile::tempdir().map_err(|error| failed("temp dir", error))?;
    let inner = Flaky {
        bus: MpscBus::start(BusConfig::default()).map_err(|error| failed("bus", error))?,
        down: Arc::new(Mutex::new(false)),
        taken: Arc::default(),
    };
    let config = SpoolConfig::with_limits(
        dir.path().join("spool"),
        NonZeroU64::new(1 << 22).ok_or_else(|| CheckFailed::new("max"))?,
        NonZeroU64::new(1 << 14).ok_or_else(|| CheckFailed::new("segment"))?,
        NonZeroUsize::new(3).ok_or_else(|| CheckFailed::new("batch"))?,
        NonZeroDuration::new(Duration::from_millis(5)).ok_or_else(|| CheckFailed::new("probe"))?,
    )
    .map_err(|error| failed("spool config", error))?;
    let spool = SpoolingBus::open(inner.clone(), config)
        .await
        .map_err(|error| failed("spool", error))?;
    let group = ConsumerGroup("live-l3-reconstruct".to_owned());
    let retry = RetryPolicy::new(
        NonZeroU32::MIN.saturating_add(9),
        Duration::from_millis(1),
        Duration::from_millis(5),
    )
    .map_err(|error| failed("retry", error))?;
    let mut subscription = spool
        .subscribe(&[Subject::Changed], group.clone(), retry)
        .await
        .map_err(|error| failed("subscribe", error))?;
    let mut held: Vec<(DeliveryId, EventId)> = Vec::new();
    let mut acked: BTreeSet<EventId> = BTreeSet::new();
    let mut published: BTreeMap<EventId, Timestamp> = BTreeMap::new();
    let now = Timestamp::from_micros(T0.as_micros() + 3_600_000_000);
    let mut next_id: u128 = 1;
    let percent = |n: u8| Probability::percent(n).map_err(|error| failed("probability", error));
    for step in 0..120 {
        if rng.chance(percent(8)?) {
            let down = !inner.is_down();
            inner.set_down(down);
            ctx.step(format!(
                "{step}: inner bus {}",
                if down { "down" } else { "up" }
            ));
        }
        if rng.chance(percent(50)?) {
            let at = Timestamp::from_micros(
                T0.as_micros() + rng.below(NonZeroU64::MIN.saturating_add(3_599_999_999)),
            );
            let id = EventId::from_ulid(next_id);
            next_id += 1;
            let envelope = Envelope {
                id,
                at,
                event: BusEvent::Changed(Changed::Agent(AgentId::from_ulid(next_id))),
            };
            if spool.publish(envelope).await.is_ok() {
                published.insert(id, at);
            }
        }
        // Take what the group has, without waiting.
        while let Ok(Some(Ok(delivery))) =
            tokio::time::timeout(Duration::from_millis(1), subscription.next()).await
        {
            held.push((delivery.id, delivery.envelope.id));
        }
        if !held.is_empty() && rng.chance(percent(40)?) {
            let index = rng.index(held.len()).unwrap_or(0);
            let (delivery, event) = held.swap_remove(index);
            if subscription.ack(delivery).await.is_ok() {
                acked.insert(event);
            }
        }
        if rng.chance(percent(30)?) {
            tokio::time::sleep(Duration::from_millis(
                rng.below(NonZeroU64::MIN.saturating_add(19)),
            ))
            .await;
        }

        // The frontier as PgFrontierSource reads it: the spool first, then
        // the group's backlog (taken by the inner bus and not acked).
        let spooled = spool.oldest_at();
        let taken = inner.taken();
        let pending: Vec<Timestamp> = taken
            .iter()
            .filter(|(id, _)| !acked.contains(id))
            .map(|(_, at)| *at)
            .collect();
        let stats = GroupStats {
            group: group.clone(),
            pending: u64::try_from(pending.len()).unwrap_or(u64::MAX),
            oldest_pending: pending.iter().min().copied(),
            dead_letters: 0,
            oldest_dead_letter: None,
        };
        let frontier = combine(
            Some(now),
            &[stats],
            &[],
            std::slice::from_ref(&group),
            spooled,
        );
        for (id, at) in &published {
            let unsent = !taken.contains_key(id);
            let unacked = !acked.contains(id);
            if unsent || unacked {
                let bound = frontier.oldest_pending;
                ctx.check(bound.is_some_and(|bound| bound <= *at), || {
                    format!(
                        "step {step}: {} at {} is {} but the frontier's oldest_pending is {bound:?}",
                        id.ulid_text(),
                        at.as_micros(),
                        if unsent { "still spooled" } else { "pending" }
                    )
                })?;
            }
        }
    }
    spool.close().await;
    inner.bus.shutdown().await;
    Ok(())
}

async fn classifier_redelivery(ctx: SimCtx) -> Result<(), CheckFailed> {
    let mut rng = ctx.rng();
    let clock = ManualClock::at(T0);
    let options = LiveConfig::new(LiveClock::Manual(clock), FlowConfig::default(), 7)
        .map_err(|error| failed("defaults", error))?
        .surface;
    let backend = InProcess::start(options)
        .await
        .map_err(|error| failed("in-process stores", error))?;
    let mut ids = Ids::seeded(u32::try_from(ctx.seed().get() % 1_000_000).unwrap_or(0));
    let mut deliveries = Vec::new();
    for index in 0..3u32 {
        let (from, to, channel) = (ids.agent(), ids.agent(), ids.channel());
        let stored = TransmissionBuilder::new(&mut ids)
            .between(from, to)
            .channel(channel)
            .opened_at(T0)
            .confirmed()
            .build()
            .map_err(|error| failed("transmission fixture", error))?;
        let transmission = stored.id;
        let at = stored
            .state
            .confirmed()
            .map(|confirmed| confirmed.at())
            .ok_or_else(|| CheckFailed::new("not confirmed"))?;
        let mut store = backend.stores.transmissions.clone();
        store
            .save(stored)
            .await
            .map_err(|error| failed("save", error))?;
        deliveries.push(Envelope {
            id: EventId::from_ulid(u128::from(index) + 1),
            at,
            event: BusEvent::Detect(DetectEvent::TransmissionConfirmed {
                transmission,
                from,
                to,
                route: Route::Channel(channel),
                at,
                matched_bytes: NonZeroU64::MIN.saturating_add(41),
            }),
        });
    }
    let mut step = Classifier::new(
        backend.stores.catalog.clone(),
        backend.stores.transmissions.clone(),
    );
    let mut first: BTreeMap<EventId, Envelope> = BTreeMap::new();
    // Seeded deliveries: each confirmation delivered one to four times,
    // in a shuffled order (redeliveries interleaved with the others).
    let mut order = Vec::new();
    for (index, _) in deliveries.iter().enumerate() {
        let times = 1 + rng.below(NonZeroU64::MIN.saturating_add(3));
        order.extend(std::iter::repeat_n(
            index,
            usize::try_from(times).unwrap_or(1),
        ));
    }
    rng.shuffle(&mut order);
    for index in order {
        let delivery = &deliveries[index];
        let out = step
            .classify(delivery)
            .await
            .map_err(|error| failed("classify", error))?
            .ok_or_else(|| CheckFailed::new("a confirmation yields an envelope"))?;
        ctx.check(
            out.id == EventId::derive(delivery.id, CLASSIFIED_LABEL, 0),
            || {
                format!(
                    "{}: the id is not derived from the delivery",
                    out.id.ulid_text()
                )
            },
        )?;
        match first.get(&delivery.id) {
            Some(earlier) => ctx.check(*earlier == out, || {
                format!(
                    "redelivery of {} published another envelope",
                    delivery.id.ulid_text()
                )
            })?,
            None => {
                first.insert(delivery.id, out);
            }
        }
    }
    backend.shutdown().await;
    Ok(())
}
