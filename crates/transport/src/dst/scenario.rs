//! A seeded scenario: groups of consumers that ack, nack, stall, ack too
//! late or crash at random while a publisher publishes, all under tokio's
//! paused clock on one thread. The run is a function of its seed: the same
//! seed gives the same log (`same_seed_replays_the_same_run`).
//!
//! A failing check names its seed; set `CROSSTALK_DST_SEED` to rerun just
//! that one.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::time::Duration;

use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, DeadLetter, DeadLetterStore, DeliveryId, EventBus, RetryPolicy,
    Subscription,
};
use crosstalk_spec::paging::{DeadLetterList, PageRequest, PageSize};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::faults::Behaviour;
use crate::rng::SplitMix64;
use crate::testing::{changed, group, non_zero, retry, watermark};
use crate::{BusConfig, DeliveryOrder, MpscBus};

pub(super) const ACK_TIMEOUT: Duration = Duration::from_millis(50);
pub(super) const INITIAL_BACKOFF: Duration = Duration::from_millis(10);
pub(super) const MAX_BACKOFF: Duration = Duration::from_millis(80);
pub(super) const MAX_ATTEMPTS: u32 = 3;
const ENVELOPES: u128 = 24;
const HORIZON: Duration = Duration::from_secs(120);

pub(super) fn scenario_retry() -> RetryPolicy {
    retry(MAX_ATTEMPTS, INITIAL_BACKOFF, MAX_BACKOFF)
}

/// One consumer group in the scenario.
struct GroupSpec {
    name: &'static str,
    subjects: &'static [Subject],
    consumers: usize,
}

static GROUPS: [GroupSpec; 2] = [
    GroupSpec {
        name: "flow",
        subjects: &[Subject::Changed, Subject::WatermarkAdvanced],
        consumers: 2,
    },
    GroupSpec {
        name: "analysis",
        subjects: &[Subject::Changed],
        consumers: 1,
    },
];

/// How one delivery ended, and when (offset from the run's start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum End {
    Acked(Duration),
    /// The ack came after the ack timeout and was refused.
    AckRefused(Duration),
    Nacked(Duration),
    /// Never settled: the ack timeout (or a crash) ends the hold.
    Ignored,
    /// The holder crashed at this offset.
    Crashed(Duration),
}

/// One delivery as a consumer saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Record {
    pub(super) group: &'static str,
    pub(super) consumer: usize,
    pub(super) incarnation: u32,
    pub(super) delivery: DeliveryId,
    pub(super) event: u128,
    pub(super) attempt: u32,
    pub(super) received: Duration,
    pub(super) end: End,
}

/// What a run produced.
pub(super) struct Outcome {
    pub(super) seed: u64,
    pub(super) published: Vec<(Envelope, Duration)>,
    pub(super) records: Vec<Record>,
    pub(super) letters: Vec<DeadLetter>,
    /// When each (consumer, incarnation) crashed, if it did.
    pub(super) crashes: HashMap<(&'static str, usize, u32), Duration>,
}

impl fmt::Debug for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {}: {} published, {} deliveries, {} dead letters",
            self.seed,
            self.published.len(),
            self.records.len(),
            self.letters.len()
        )
    }
}

/// The seeds a check runs: `CROSSTALK_DST_SEED` alone when set, else
/// `0..count`.
pub(super) fn seeds(count: u64) -> Vec<u64> {
    match std::env::var("CROSSTALK_DST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(seed) => vec![seed],
        None => (0..count).collect(),
    }
}

/// Run every seed and apply `check` to each outcome, failing with the seed.
pub(super) async fn for_seeds(count: u64, check: impl Fn(&Outcome) -> Result<(), String>) {
    for seed in seeds(count) {
        let outcome = run(seed).await;
        if let Err(failure) = check(&outcome) {
            panic!("{outcome:?}: {failure}\nrerun with CROSSTALK_DST_SEED={seed}");
        }
    }
}

enum Event {
    Record(Record),
    Crash(&'static str, usize, u32, Duration),
}

/// Run the scenario for `seed`.
pub(super) async fn run(seed: u64) -> Outcome {
    let start = Instant::now();
    let since = move || start.elapsed();
    let config = BusConfig {
        ack_timeout: non_zero(ACK_TIMEOUT),
        retry: scenario_retry(),
        order: DeliveryOrder::Shuffled { seed },
        ..BusConfig::default()
    };
    let bus = MpscBus::start(config).expect("bus starts");
    let (log, mut logged) = mpsc::unbounded_channel();

    let mut consumers = Vec::new();
    for spec in &GROUPS {
        for consumer in 0..spec.consumers {
            let sub = bus
                .subscribe(spec.subjects, group(spec.name), scenario_retry())
                .await
                .expect("subscribe");
            let rng =
                SplitMix64::new(seed ^ ((consumer as u64 + 1) << 32) ^ spec.name.len() as u64);
            consumers.push(tokio::spawn(consume(
                bus.clone(),
                spec,
                consumer,
                sub,
                rng,
                log.clone(),
                since,
            )));
        }
    }
    drop(log);

    let mut publisher_rng = SplitMix64::new(seed.wrapping_mul(31).wrapping_add(7));
    let mut published = Vec::new();
    for n in 1..=ENVELOPES {
        let pause = publisher_rng.below(30) as u64;
        tokio::time::sleep(Duration::from_millis(pause)).await;
        let envelope = if publisher_rng.below(3) == 0 {
            watermark(n)
        } else {
            changed(n)
        };
        bus.publish(envelope.clone()).await.expect("publish");
        published.push((envelope, since()));
    }

    tokio::time::sleep(HORIZON).await;
    let letters = all_letters(&bus).await;
    bus.shutdown().await;
    for consumer in consumers {
        consumer.await.expect("consumer task ends");
    }
    let mut records = Vec::new();
    let mut crashes = HashMap::new();
    while let Some(event) = logged.recv().await {
        match event {
            Event::Record(record) => records.push(record),
            Event::Crash(group, consumer, incarnation, at) => {
                crashes.insert((group, consumer, incarnation), at);
            }
        }
    }
    Outcome {
        seed,
        published,
        records,
        letters,
        crashes,
    }
}

async fn all_letters(bus: &MpscBus) -> Vec<DeadLetter> {
    let store = bus.dead_letters();
    let mut letters = Vec::new();
    let mut after = None;
    loop {
        let request = PageRequest::<DeadLetterList> {
            size: PageSize::new(PageSize::MAX).expect("max is valid"),
            after,
        };
        let page = store.list(None, &request).await.expect("lists");
        letters.extend(page.items().iter().cloned());
        match page.next() {
            Some(next) => after = Some(next.clone()),
            None => return letters,
        }
    }
}

async fn consume(
    bus: MpscBus,
    spec: &'static GroupSpec,
    consumer: usize,
    mut sub: crate::MpscSubscription,
    mut rng: SplitMix64,
    log: mpsc::UnboundedSender<Event>,
    since: impl Fn() -> Duration,
) {
    let mut incarnation = 0;
    loop {
        let delivery = match sub.next().await {
            Some(Ok(delivery)) => delivery,
            Some(Err(error)) => panic!("unexpected bus error: {error:?}"),
            None => return,
        };
        let received = since();
        let behaviour = Behaviour::pick(&mut rng);
        tokio::time::sleep(behaviour.handling(&mut rng, ACK_TIMEOUT)).await;
        let end = match behaviour {
            Behaviour::Ack | Behaviour::SlowAck => match sub.ack(delivery.id).await {
                Ok(()) => End::Acked(since()),
                Err(BusError::UnknownDelivery(_)) => End::AckRefused(since()),
                Err(error) => panic!("unexpected ack error: {error:?}"),
            },
            Behaviour::Nack => {
                let retry_after = Duration::from_millis(rng.below(120) as u64);
                match sub
                    .nack(delivery.id, retry_after, format!("nack {}", delivery.id.0))
                    .await
                {
                    Ok(()) => End::Nacked(since()),
                    Err(BusError::UnknownDelivery(_)) => End::AckRefused(since()),
                    Err(error) => panic!("unexpected nack error: {error:?}"),
                }
            }
            Behaviour::Ignore => End::Ignored,
            Behaviour::Crash => End::Crashed(since()),
        };
        let record = Record {
            group: spec.name,
            consumer,
            incarnation,
            delivery: delivery.id,
            event: delivery.envelope.id.as_ulid(),
            attempt: delivery.attempt.get(),
            received,
            end,
        };
        let _ = log.send(Event::Record(record));
        if let End::Crashed(at) = end {
            drop(sub);
            let _ = log.send(Event::Crash(spec.name, consumer, incarnation, at));
            incarnation += 1;
            tokio::time::sleep(Duration::from_millis(rng.below(200) as u64)).await;
            sub = match bus
                .subscribe(spec.subjects, group(spec.name), scenario_retry())
                .await
            {
                Ok(sub) => sub,
                Err(BusError::Disconnected) => return,
                Err(error) => panic!("resubscribe failed: {error:?}"),
            };
        }
    }
}

impl Outcome {
    /// Every (group, event) the run owes: each published envelope to each
    /// group subscribed to its subject.
    pub(super) fn owed(&self) -> Vec<(&'static str, u128)> {
        let mut owed = Vec::new();
        for spec in &GROUPS {
            for (envelope, _) in &self.published {
                if spec.subjects.contains(&envelope.event.subject()) {
                    owed.push((spec.name, envelope.id.as_ulid()));
                }
            }
        }
        owed
    }

    /// Deliveries per (group, event), in the order they were received.
    pub(super) fn by_envelope(&self) -> BTreeMap<(&'static str, u128), Vec<&Record>> {
        let mut map: BTreeMap<_, Vec<&Record>> = BTreeMap::new();
        for record in &self.records {
            map.entry((record.group, record.event))
                .or_default()
                .push(record);
        }
        for records in map.values_mut() {
            records.sort_by_key(|r| (r.received, r.delivery.0));
        }
        map
    }

    pub(super) fn letter(&self, group: &str, event: u128) -> Option<&DeadLetter> {
        self.letters.iter().find(|l| {
            l.group == ConsumerGroup(group.to_owned()) && l.envelope.id.as_ulid() == event
        })
    }

    pub(super) fn published(&self, event: u128) -> Option<&Envelope> {
        self.published
            .iter()
            .map(|(e, _)| e)
            .find(|e| e.id.as_ulid() == event)
    }

    /// When the holder stopped holding this delivery.
    pub(super) fn hold_end(&self, record: &Record) -> Duration {
        let crashed = self
            .crashes
            .get(&(record.group, record.consumer, record.incarnation))
            .copied()
            .filter(|at| *at >= record.received);
        let expiry = record.received + ACK_TIMEOUT;
        match record.end {
            End::Acked(at) | End::Nacked(at) | End::Crashed(at) => at,
            End::AckRefused(_) | End::Ignored => crashed.map_or(expiry, |at| at.min(expiry)),
        }
    }
}
