//! Deterministic simulation tests of the topology consumer: tokio's paused
//! clock, the in-process bus (`crosstalk-transport`), and crosstalk-memory's
//! reference edge store behind a probe that logs every call. The consumer
//! is generic over `EdgeStore`, so these pin its delivery handling and its
//! cadence; the store's behaviour is the model test's.

mod restart;

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crosstalk_memory::analysis::aliases::StaticDirectory;
use crosstalk_memory::model::build::{agent, bucket_width, catalog, timing, transmission, ts};
use crosstalk_memory::support::Outbox;
use crosstalk_memory::topology::env::{Env, StaticNodes};
use crosstalk_memory::topology::store::{EdgeStoreConfig, InMemoryEdgeStore, ManualFrontier};
use crosstalk_spec::aggregates::access::{AccessEdge, BipartiteGraph};
use crosstalk_spec::aggregates::agents::AgentTraffic;
use crosstalk_spec::aggregates::edge::{
    EdgeKey, EdgeSelector, EdgeTotals, EdgeTransmissionPage, TopologyFilter, TopologyGraph,
    Weighting,
};
use crosstalk_spec::aggregates::series::{BucketWidth, SeriesGrid, SeriesGrouping, TopologySeries};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::{PipelineFrontier, Watermark, Watermarked};
use crosstalk_spec::derived::flow::transmission::{Classification, Route};
use crosstalk_spec::derived::flow::verdict::{Observed, Verdict, VerdictRevision};
use crosstalk_spec::events::insight::{ClassificationCause, InsightEvent};
use crosstalk_spec::events::{BusEvent, Envelope};
use crosstalk_spec::ids::{AgentId, EventId, SeededRandom, TransmissionId, UlidGenerator};
use crosstalk_spec::interfaces::l2_transport::{BusError, EventBus, RetryPolicy};
use crosstalk_spec::interfaces::l7_topology::{
    AccessContribution, Activation, EdgeContribution, EdgeError, EdgeQueryError, EdgeStore,
};
use crosstalk_spec::paging::{EdgeTransmissionList, PageRequest};
use crosstalk_spec::support::{Clock, TimeWindow, Timestamp};
use crosstalk_transport::{BusConfig, MpscBus};
use tokio::time::Instant;

use crate::consumer::{ConsumerSettings, SUBJECTS, group, run};
use crate::outbox::Announce;

type Reference = InMemoryEdgeStore<
    Env<crosstalk_memory::analysis::catalog::InMemoryTopicCatalog, StaticDirectory, StaticNodes>,
>;

/// What the probe and the announcer saw, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    /// `apply` returned this.
    Applied(Result<EdgeKey, EdgeError>),
    /// `advance_watermark` was called at this paused-clock offset.
    Advanced(Duration),
    /// `EdgeUpdated` reached the announcer and was published.
    Published(EdgeKey),
    /// Publishing `EdgeUpdated` failed (a crash before the publish).
    PublishFailed(EdgeKey),
}

type Log = Arc<Mutex<Vec<Step>>>;

fn record(log: &Log, step: Step) {
    log.lock().expect("not poisoned").push(step);
}

fn steps(log: &Log) -> Vec<Step> {
    log.lock().expect("not poisoned").clone()
}

/// The reference store, logging applies and watermark recomputations.
#[derive(Clone)]
struct Probe {
    inner: Reference,
    log: Log,
    started: Instant,
}

impl EdgeStore for Probe {
    async fn apply(&mut self, contribution: &EdgeContribution) -> Result<EdgeKey, EdgeError> {
        let result = self.inner.apply(contribution).await;
        record(&self.log, Step::Applied(result.clone()));
        result
    }

    fn judge(
        &mut self,
        transmission: TransmissionId,
        verdict: Option<Verdict>,
        revision: VerdictRevision,
    ) -> impl Future<Output = Result<Observed, EdgeError>> + Send {
        self.inner.judge(transmission, verdict, revision)
    }

    fn version_ready(
        &mut self,
        version: TopicModelVersion,
        transmissions: u64,
    ) -> impl Future<Output = Result<(), EdgeError>> + Send {
        self.inner.version_ready(version, transmissions)
    }

    fn activate(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<Activation, EdgeError>> + Send {
        self.inner.activate(version)
    }

    fn drop_version(
        &mut self,
        version: TopicModelVersion,
    ) -> impl Future<Output = Result<(), EdgeError>> + Send {
        self.inner.drop_version(version)
    }

    fn watermark(&self) -> impl Future<Output = Result<Watermark, EdgeQueryError>> + Send {
        self.inner.watermark()
    }

    async fn advance_watermark(
        &mut self,
        frontier: PipelineFrontier,
    ) -> Result<Option<Watermark>, EdgeError> {
        record(&self.log, Step::Advanced(self.started.elapsed()));
        self.inner.advance_watermark(frontier).await
    }

    fn apply_access(
        &mut self,
        access: &AccessContribution,
    ) -> impl Future<Output = Result<AccessEdge, EdgeError>> + Send {
        self.inner.apply_access(access)
    }

    fn graph(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologyGraph>, EdgeQueryError>> + Send {
        self.inner.graph(window, weighting, filter)
    }

    fn totals(
        &self,
        window: TimeWindow,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<EdgeTotals>, EdgeQueryError>> + Send {
        self.inner.totals(window, filter)
    }

    fn channel_topology(
        &self,
        window: TimeWindow,
        weighting: Weighting,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<BipartiteGraph>, EdgeQueryError>> + Send {
        self.inner.channel_topology(window, weighting, filter)
    }

    fn transmissions(
        &self,
        edge: &EdgeSelector,
        window: TimeWindow,
        filter: &TopologyFilter,
        page: &PageRequest<EdgeTransmissionList>,
    ) -> impl Future<Output = Result<Watermarked<EdgeTransmissionPage>, EdgeQueryError>> + Send
    {
        self.inner.transmissions(edge, window, filter, page)
    }

    fn agent_traffic(
        &self,
        window: TimeWindow,
        agents: &[AgentId],
    ) -> impl Future<Output = Result<Watermarked<BTreeMap<AgentId, AgentTraffic>>, EdgeQueryError>> + Send
    {
        self.inner.agent_traffic(window, agents)
    }

    fn bucket_width(&self) -> BucketWidth {
        self.inner.bucket_width()
    }

    fn series(
        &self,
        grid: SeriesGrid,
        weighting: Weighting,
        grouping: SeriesGrouping,
        filter: &TopologyFilter,
    ) -> impl Future<Output = Result<Watermarked<TopologySeries>, EdgeQueryError>> + Send {
        self.inner.series(grid, weighting, grouping, filter)
    }
}

/// The envelopes the consumer's announcer was handed, failed or not.
type Sent = Arc<Mutex<Vec<Envelope>>>;

/// The consumer's announcer: logs each `EdgeUpdated`, failing the first
/// `failures` of them (the process dies before the publish lands).
struct Announcer {
    log: Log,
    sent: Sent,
    failures: Mutex<u32>,
}

impl Announce for Announcer {
    async fn announce(&self, envelope: Envelope) -> Result<(), BusError> {
        self.sent
            .lock()
            .expect("not poisoned")
            .push(envelope.clone());
        let BusEvent::Insight(InsightEvent::EdgeUpdated(key)) = envelope.event else {
            return Ok(());
        };
        let fail = {
            let mut left = self.failures.lock().expect("not poisoned");
            let fail = *left > 0;
            *left = left.saturating_sub(1);
            fail
        };
        if fail {
            record(&self.log, Step::PublishFailed(key));
            return Err(BusError::Disconnected);
        }
        record(&self.log, Step::Published(key));
        Ok(())
    }
}

/// A clock the paused runtime drives: the epoch plus tokio's elapsed time.
struct PausedClock(Instant);

impl Clock for PausedClock {
    fn now(&self) -> Timestamp {
        let micros = u64::try_from(self.0.elapsed().as_micros()).unwrap_or(u64::MAX);
        ts(1_000_000 + micros)
    }
}

/// A running consumer over a fresh bus and reference store.
struct Sim {
    bus: MpscBus,
    /// Mints the ids of the envelopes the test publishes.
    ids: tokio::sync::Mutex<UlidGenerator<SeededRandom>>,
    clock: Arc<dyn Clock>,
    store: Reference,
    log: Log,
    sent: Sent,
}

/// 10 ms buckets; `settle_after` 20 µs (the watermark is not under test
/// except for its cadence).
const WIDTH_MICROS: u64 = 10_000;

async fn start(failures: u32) -> Sim {
    let started = Instant::now();
    let bus = MpscBus::start(BusConfig::default()).expect("a bus");
    let retry = RetryPolicy::new(
        std::num::NonZeroU32::new(5).expect("non-zero"),
        Duration::from_millis(50),
        Duration::from_secs(1),
    )
    .expect("a retry policy");
    let subscription = bus
        .subscribe(&SUBJECTS, group(), retry)
        .await
        .expect("subscribed");
    let catalog = catalog(3, 0.5, Outbox::none()).expect("a catalog");
    let env = Env {
        topics: catalog,
        directory: StaticDirectory::new(),
        nodes: StaticNodes::new(),
    };
    let config = EdgeStoreConfig {
        bucket_width: bucket_width(WIDTH_MICROS),
        timing: timing(20).expect("a timing"),
    };
    let store = InMemoryEdgeStore::new(config, env, Outbox::none());
    let log: Log = Arc::default();
    let probe = Probe {
        inner: store.clone(),
        log: Arc::clone(&log),
        started,
    };
    let sent: Sent = Arc::default();
    let announcer = Announcer {
        log: Arc::clone(&log),
        sent: Arc::clone(&sent),
        failures: Mutex::new(failures),
    };
    let frontier = ManualFrontier::new(PipelineFrontier {
        ticked_through: ts(0),
        oldest_pending: None,
    });
    let settings =
        ConsumerSettings::for_bucket_width(NonZeroU64::new(WIDTH_MICROS).expect("non-zero"));
    tokio::spawn(run(subscription, probe, frontier, announcer, settings));
    let clock: Arc<dyn Clock> = Arc::new(PausedClock(started));
    let ids = tokio::sync::Mutex::new(UlidGenerator::new(Arc::clone(&clock), SeededRandom::new(7)));
    Sim {
        bus,
        ids,
        clock,
        store,
        log,
        sent,
    }
}

fn classified(n: u64, from: u64, to: u64) -> BusEvent {
    BusEvent::Insight(InsightEvent::TransmissionClassified {
        cause: ClassificationCause::Confirmation,
        transmission: transmission(n),
        from: agent(from),
        to: agent(to),
        route: Route::Unobserved,
        at: ts(5),
        matched_bytes: NonZeroU64::new(3).expect("non-zero"),
        classification: Classification {
            version: TopicModelVersion(0),
            topic: None,
            watched: false,
        },
    })
}

impl Sim {
    /// Publish `event` under a fresh id; returns its envelope.
    async fn publish(&self, event: BusEvent) -> Envelope {
        let at = self.clock.now();
        let id: EventId = self.ids.lock().await.mint_at(at).expect("an id");
        let envelope = Envelope { id, at, event };
        self.bus.publish(envelope.clone()).await.expect("published");
        envelope
    }

    /// Publish `envelope` again, as a bus redelivers.
    async fn republish(&self, envelope: &Envelope) {
        self.bus.publish(envelope.clone()).await.expect("published");
    }

    fn sent(&self) -> Vec<Envelope> {
        self.sent.lock().expect("not poisoned").clone()
    }

    /// Let every delivery, retry and backoff run out.
    async fn settle(&self) {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    async fn group_is_empty(&self) -> bool {
        let depth = self
            .bus
            .depth(&group())
            .await
            .expect("a depth")
            .expect("the group exists");
        depth.ready + depth.delayed + depth.held + depth.exhausted + depth.waiting == 0
    }

    fn applies(&self) -> Vec<Result<EdgeKey, EdgeError>> {
        steps(&self.log)
            .into_iter()
            .filter_map(|step| match step {
                Step::Applied(result) => Some(result),
                _ => None,
            })
            .collect()
    }
}

/// topology.consumer.acks-self-edge
#[tokio::test(start_paused = true)]
async fn self_edge_delivery_is_acked_once_and_not_redelivered() {
    let sim = start(0).await;
    sim.publish(classified(1, 4, 4)).await;
    sim.settle().await;
    assert_eq!(sim.applies(), vec![Err(EdgeError::SelfEdge)]);
    assert!(sim.group_is_empty().await);
    assert!(
        !steps(&sim.log)
            .iter()
            .any(|step| matches!(step, Step::Published(_) | Step::PublishFailed(_)))
    );
}

/// topology.consumer.ack-after-publish: the first attempt applies and then
/// fails to publish `EdgeUpdated`; the delivery is not acked, so it comes
/// back, the apply repeats idempotently and the publish lands.
#[tokio::test(start_paused = true)]
async fn crash_between_apply_and_publish_still_publishes() {
    let sim = start(1).await;
    sim.publish(classified(1, 1, 2)).await;
    sim.settle().await;
    let applies = sim.applies();
    assert_eq!(
        applies.len(),
        2,
        "applied, then applied again on redelivery"
    );
    let key = applies[0].clone().expect("applied");
    assert_eq!(applies[1], Ok(key.clone()));
    let published: Vec<Step> = steps(&sim.log)
        .into_iter()
        .filter(|step| matches!(step, Step::Published(_) | Step::PublishFailed(_)))
        .collect();
    assert_eq!(
        published,
        vec![Step::PublishFailed(key.clone()), Step::Published(key)]
    );
    assert!(sim.group_is_empty().await);
    let graph = sim
        .store
        .graph(
            TimeWindow::new(ts(0), ts(WIDTH_MICROS)).expect("a window"),
            Weighting::Transmissions,
            &TopologyFilter::default(),
        )
        .await
        .expect("a graph");
    assert_eq!(graph.value.total(), 1, "counted once");
}

/// topology.edge-updated.after-apply
#[tokio::test(start_paused = true)]
async fn edge_updated_never_precedes_persisted_apply() {
    let sim = start(2).await;
    for n in 1..=6 {
        sim.publish(classified(n, n % 3, 1 + n % 2)).await;
        // A redelivery of the same transmission, as the bus may do.
        sim.publish(classified(n, n % 3, 1 + n % 2)).await;
    }
    sim.settle().await;
    let log = steps(&sim.log);
    let mut applied = Vec::new();
    let mut published = 0;
    for step in &log {
        match step {
            Step::Applied(Ok(key)) => applied.push(key.clone()),
            Step::Published(key) | Step::PublishFailed(key) => {
                assert!(
                    applied.contains(key),
                    "EdgeUpdated({key:?}) before its apply returned"
                );
                published += 1;
            }
            Step::Applied(Err(_)) | Step::Advanced(_) => {}
        }
    }
    assert!(published > 0);
    assert!(sim.group_is_empty().await);
}

/// topology.watermark.recompute-cadence
#[tokio::test(start_paused = true)]
async fn watermark_recomputed_every_bucket() {
    let sim = start(0).await;
    // Deliveries keep arriving while the clock runs: the cadence holds.
    for n in 1..=50 {
        sim.publish(classified(n, 1, 2)).await;
        tokio::time::sleep(Duration::from_millis(7)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ticks: Vec<Duration> = steps(&sim.log)
        .into_iter()
        .filter_map(|step| match step {
            Step::Advanced(at) => Some(at),
            _ => None,
        })
        .collect();
    let width = Duration::from_micros(WIDTH_MICROS);
    assert!(ticks.len() >= 80, "only {} recomputations", ticks.len());
    assert!(ticks[0] <= width);
    for pair in ticks.windows(2) {
        assert!(
            pair[1] - pair[0] <= width,
            "a gap of {:?}",
            pair[1] - pair[0]
        );
    }
}

/// transport.consumer.derived-envelope-ids: the `EdgeUpdated` a delivery
/// causes has an id derived from the delivery's envelope, so its first
/// attempt (whose publish failed), its redelivery after a nack and a
/// duplicate delivery of the same envelope all publish one envelope, id
/// and time included; another delivery gets another id.
#[tokio::test(start_paused = true)]
async fn redelivery_republishes_the_same_envelope_ids() {
    let sim = start(1).await;
    let first = sim.publish(classified(1, 1, 2)).await;
    sim.settle().await;
    // The bus delivers the same envelope again, as it may after a crash
    // between the publish and the ack.
    sim.republish(&first).await;
    sim.settle().await;
    let second = sim.publish(classified(2, 1, 2)).await;
    sim.settle().await;
    assert!(sim.group_is_empty().await);

    let sent = sim.sent();
    assert_eq!(sent.len(), 4, "failed, redelivered, duplicate, second");
    let derived = EventId::derive(first.id, crate::consumer::EDGE_UPDATED, 0);
    for envelope in &sent[..3] {
        assert_eq!(envelope.id, derived);
        assert_eq!(envelope.at, first.at);
        assert_eq!(envelope, &sent[0]);
    }
    assert_eq!(
        sent[3].id,
        EventId::derive(second.id, crate::consumer::EDGE_UPDATED, 0)
    );
    assert_ne!(sent[3].id, derived);
    assert_eq!(
        sent[0],
        crate::consumer::edge_updated(
            &first,
            match &sent[0].event {
                BusEvent::Insight(InsightEvent::EdgeUpdated(key)) => key.clone(),
                other => panic!("not an EdgeUpdated: {other:?}"),
            }
        )
    );
}

/// topology.watermark.monotone: seeded advances with restarts over
/// Postgres ([`restart`]); a restarted store reads the persisted watermark
/// and nothing lowers it.
#[tokio::test(flavor = "multi_thread")]
async fn watermark_never_decreases_across_restarts() {
    restart::seeded_restarts_never_lower_the_watermark().await;
}
