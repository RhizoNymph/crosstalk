//! The relay's settle barrier, the evidence records read from the
//! registry, and the opt-in projection fitter.

use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_memory::analysis::fakes::FakeLayoutFitter;
use crosstalk_memory::analysis::projection::{InMemoryProjectionStore, ProjectionConfig};
use crosstalk_memory::model::build::{agent, operator, projection, test_model, transmission};
use crosstalk_memory::support::{ManualClock, Outbox};
use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::aggregates::projection::{
    FitFailure, PointRoute, ProjectionInfo, ProjectionLimit, ProjectionParams, ProjectionSpec,
    ProjectionStatus, ProjectionStatusKind,
};
use crosstalk_spec::aggregates::topic::{Embedding, TopicModelVersion};
use crosstalk_spec::derived::flow::channel::confirmation::{Confirmation, Listing};
use crosstalk_spec::interfaces::l5_flow::Discovery;
use crosstalk_spec::interfaces::l5_flow::channels::ChannelTraffic;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStore;
use crosstalk_spec::interfaces::l6_analysis::{
    ProjectionSource, ProjectionStore, Sample, SampleError, SampleRow,
};
use crosstalk_spec::interfaces::l7_topology::NodeFacts;
use crosstalk_spec::support::{Clock, TimeWindow, Timestamp, Watermark};
use crosstalk_surface::EvidenceRecords;
use crosstalk_testkit::build::{ResourceBuilder, TransmissionBuilder};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;

use super::options;
use crate::InProcess;
use crate::in_process::fitting::Fitter;

/// INV-1074, INV-1076: once `settle` returns, the node facts hold every event the
/// stores published before it, with no other yield in between: a channel
/// discovered from a resource accessed before it existed is known, listed
/// and holds that resource. The evidence page reads the accesses and the
/// resource from the registry that recorded them.
#[tokio::test]
async fn settle_applies_every_event_published_before_it() {
    let clock = ManualClock::at(T0);
    let backend = match InProcess::start(options(clock)).await {
        Ok(backend) => backend,
        Err(error) => panic!("start: {error}"),
    };
    let mut ids = Ids::seeded(5);
    let (writer, reader) = (ids.agent(), ids.agent());
    let resource = ResourceBuilder::new(&mut ids)
        .url("https", "wiki.example", "/page", None)
        .first_seen(T0)
        .build();
    let channel = ids.channel();
    let parts = match TransmissionBuilder::new(&mut ids)
        .between(writer, reader)
        .channel(channel)
        .accesses(|cross| cross.resource(resource.id))
        .awaiting_content()
        .build_parts()
    {
        Ok(parts) => parts,
        Err(error) => panic!("transmission: {error:?}"),
    };
    let mut registry = backend.stores.channels.clone();
    assert_eq!(registry.add_resource(resource.clone()).await, Ok(None));
    for access in [&parts.write, &parts.read] {
        assert!(registry.record_access(access.clone()).await.is_ok());
    }
    let opened = &parts.transmission;
    assert_eq!(
        registry
            .discover(channel, resource.id, opened.id, opened.opened_at)
            .await,
        Ok(Discovery::Created(channel))
    );
    let mut transmissions = backend.stores.transmissions.clone();
    assert!(transmissions.save(opened.clone()).await.is_ok());
    assert!(registry.record_transmission(opened).await.is_ok());

    assert_eq!(backend.settle().await.map_err(|e| e.to_string()), Ok(()));
    let nodes = &backend.stores.nodes;
    assert_eq!(nodes.channel_of(resource.id), Some(channel));
    assert_eq!(
        nodes.channel(channel).map(|facts| facts.listing),
        Some(Listing::Channel(Confirmation::Unconfirmed))
    );

    let evidence = &backend.stores.evidence;
    assert_eq!(
        evidence.access(parts.write.id).await,
        Ok(Some(parts.write.clone()))
    );
    assert_eq!(
        evidence.resource(resource.id).await,
        Ok(Some(resource.clone()))
    );
    assert_eq!(evidence.access(ids.access()).await, Ok(None));
    backend.shutdown().await;
}

/// What a sample read answers, set by the test.
#[derive(Clone)]
struct FixedSource(Arc<Mutex<Result<Sample, SampleError>>>);

impl FixedSource {
    fn new(answer: Result<Sample, SampleError>) -> Self {
        Self(Arc::new(Mutex::new(answer)))
    }
}

impl ProjectionSource for FixedSource {
    async fn sample(&self, _spec: &ProjectionSpec) -> Result<Sample, SampleError> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn spec(limit: u32) -> ProjectionSpec {
    let (Ok(limit), Ok(window)) = (
        ProjectionLimit::new(limit),
        TimeWindow::new(T0, Timestamp::from_micros(T0.as_micros() + 1_000_000)),
    ) else {
        panic!("spec parts");
    };
    let Ok(params) = ProjectionParams::new(limit, 2, 100, 7) else {
        panic!("params");
    };
    ProjectionSpec::new(
        window,
        TopologyFilter::default(),
        TopicModelVersion(0),
        params,
        test_model("dev"),
    )
}

/// `n` rows between two agents, the `i`th embedded at the unit vector
/// (cos i, sin i, 0).
fn sample(n: u64, matching: u64) -> Sample {
    let rows = (0..n)
        .map(|i| {
            let angle = i as f32;
            let Ok(embedding) =
                Embedding::new(test_model("dev"), vec![angle.cos(), angle.sin(), 0.0])
            else {
                panic!("embedding");
            };
            SampleRow {
                transmission: transmission(i + 1),
                from: agent(1),
                to: agent(2),
                route: PointRoute::Direct,
                topic: None,
                confirmed_at: Timestamp::from_micros(T0.as_micros() + i),
                embedding,
            }
        })
        .collect();
    Sample {
        watermark: Watermark(T0),
        matching,
        rows,
    }
}

fn fitter(
    answer: Result<Sample, SampleError>,
    clock: &ManualClock,
) -> Fitter<InMemoryProjectionStore, FixedSource, FakeLayoutFitter> {
    Fitter {
        jobs: InMemoryProjectionStore::new(
            ProjectionConfig {
                lease: std::time::Duration::from_secs(60),
                frame_retention: std::time::Duration::from_secs(3600),
            },
            Outbox::none(),
        ),
        source: FixedSource::new(answer),
        fitter: FakeLayoutFitter,
        clock: Arc::new(clock.clone()),
    }
}

async fn status(jobs: &InMemoryProjectionStore, n: u64) -> ProjectionInfo {
    match jobs.info(projection(n)).await {
        Ok(Some(info)) => info,
        other => panic!("info: {other:?}"),
    }
}

/// INV-1073: a pass claims every queued job, oldest first, lays out its
/// sample with the deterministic fitter and stores the frame: the job is
/// ready with the sample's watermark and counts, and the same sample lays
/// out the same frame again.
#[tokio::test]
async fn a_pass_fits_every_queued_job() {
    let clock = ManualClock::at(T0);
    let mut run = fitter(Ok(sample(5, 9)), &clock);
    for n in [1, 2] {
        let queued = ProjectionInfo::queued(projection(n), spec(5), operator(1), clock.now());
        assert_eq!(run.jobs.enqueue(queued).await, Ok(()));
    }
    assert_eq!(run.pass().await, Ok(2));
    let (first, second) = (status(&run.jobs, 1).await, status(&run.jobs, 2).await);
    let ProjectionStatus::Ready(fitted) = first.status() else {
        panic!("ready: {:?}", first.status());
    };
    assert_eq!(fitted.watermark, Watermark(T0));
    assert_eq!((fitted.matching, fitted.points), (9, 5));
    let (Ok(a), Ok(b)) = (
        run.jobs.projection(projection(1)).await,
        run.jobs.projection(projection(2)).await,
    ) else {
        panic!("frames");
    };
    assert_eq!(second.status().kind(), ProjectionStatusKind::Ready);
    assert_eq!(a.frame().columns(), b.frame().columns());
    assert_eq!(run.pass().await, Ok(0), "nothing left queued");
}

/// A sample with too few points fails the job with the fitter's
/// `TooFewPoints`; a sample the source refuses fails it with that refusal.
#[tokio::test]
async fn deterministic_refusals_fail_the_job() {
    let clock = ManualClock::at(T0);
    let mut run = fitter(Ok(sample(0, 0)), &clock);
    let queued = ProjectionInfo::queued(projection(1), spec(5), operator(1), clock.now());
    assert_eq!(run.jobs.enqueue(queued).await, Ok(()));
    assert_eq!(run.pass().await, Ok(1));
    assert!(matches!(
        status(&run.jobs, 1).await.status(),
        ProjectionStatus::Failed {
            failure: FitFailure::TooFewPoints { needed: 3, got: 0 },
            ..
        }
    ));

    let refused = FitFailure::VersionNotRetained {
        version: TopicModelVersion(0),
    };
    let mut run = fitter(Err(SampleError::Failed(refused.clone())), &clock);
    let queued = ProjectionInfo::queued(projection(2), spec(5), operator(1), clock.now());
    assert_eq!(run.jobs.enqueue(queued).await, Ok(()));
    assert_eq!(run.pass().await, Ok(1));
    assert!(matches!(
        status(&run.jobs, 2).await.status(),
        ProjectionStatus::Failed { failure, .. } if *failure == refused
    ));
}

/// A store failure while sampling leaves the job fitting; once its lease
/// lapses the next pass requeues and fits it.
#[tokio::test]
async fn a_transient_failure_leaves_the_job_to_its_lease() {
    let clock = ManualClock::at(T0);
    let mut run = fitter(
        Err(SampleError::Store {
            reason: "unavailable".to_owned(),
        }),
        &clock,
    );
    let queued = ProjectionInfo::queued(projection(1), spec(5), operator(1), clock.now());
    assert_eq!(run.jobs.enqueue(queued).await, Ok(()));
    assert_eq!(run.pass().await, Ok(0));
    assert_eq!(
        status(&run.jobs, 1).await.status().kind(),
        ProjectionStatusKind::Fitting
    );
    *run.source.0.lock().unwrap_or_else(PoisonError::into_inner) = Ok(sample(4, 4));
    clock.set(Timestamp::from_micros(T0.as_micros() + 61_000_000));
    assert_eq!(run.pass().await, Ok(1));
    assert_eq!(
        status(&run.jobs, 1).await.status().kind(),
        ProjectionStatusKind::Ready
    );
}
