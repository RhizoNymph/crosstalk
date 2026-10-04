//! `InMemoryProjectionStore`: the job lifecycle, the queue bound, leases
//! and frame retention.

use std::time::Duration;

use crosstalk_spec::aggregates::filter::TopologyFilter;
use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    FitFailure, InvalidProjectionInfo, InvalidTransition, PointRoute, ProjectedPoint,
    ProjectionInfo, ProjectionLimit, ProjectionMismatch, ProjectionParams, ProjectionSpec,
    ProjectionStatus, ProjectionStatusKind,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::Watermark;
use crosstalk_spec::events::BusEvent;
use crosstalk_spec::events::changed::Changed;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l6_analysis::{
    ProjectionJobError, ProjectionStore, ProjectionStoreError,
};
use crosstalk_spec::paging::{PageRequest, PageSize, ProjectionList};
use crosstalk_spec::support::Finite;

use super::support::{at, model};
use crate::analysis::projection::{InMemoryProjectionStore, ProjectionConfig};
use crate::model::build::{agent, operator, projection, transmission, window};
use crate::support::{Outbox, drain};

const LEASE: u64 = 100;
const RETENTION: u64 = 1_000;

fn config() -> ProjectionConfig {
    ProjectionConfig {
        lease: Duration::from_micros(LEASE),
        frame_retention: Duration::from_micros(RETENTION),
    }
}

fn store() -> InMemoryProjectionStore {
    InMemoryProjectionStore::new(config(), Outbox::none())
}

fn spec(limit: u32) -> ProjectionSpec {
    let params = ProjectionParams::new(ProjectionLimit::new(limit).unwrap(), 2, 100, 7).unwrap();
    ProjectionSpec::new(
        window(0, 1_000).unwrap(),
        TopologyFilter::default(),
        TopicModelVersion(0),
        params,
        model(),
    )
}

fn job(n: u64, requested: u64) -> ProjectionInfo {
    ProjectionInfo::queued(projection(n), spec(10), operator(1), at(requested))
}

/// A frame for job `id` with `points` points of `matching`, watermark `w`.
fn frame(id: ProjectionId, points: u64, matching: u64, w: u64) -> ProjectionFrame {
    let header = FrameHeader {
        projection: id,
        topic_version: TopicModelVersion(0),
        watermark: Watermark(at(w)),
        limit: ProjectionLimit::new(10).unwrap(),
        matching,
    };
    let points: Vec<ProjectedPoint> = (0..points)
        .map(|n| ProjectedPoint {
            transmission: transmission(n),
            from: agent(1),
            to: agent(2),
            route: PointRoute::Direct,
            topic: None,
            confirmed_at: at(n),
            x: Finite::new(0.5).unwrap(),
            y: Finite::new(-0.5).unwrap(),
        })
        .collect();
    ProjectionFrame::from_points(header, &points).unwrap()
}

fn status(info: &ProjectionInfo) -> ProjectionStatusKind {
    info.status().kind()
}

#[tokio::test]
async fn job_runs_queued_fitting_ready_expired() {
    let (outbox, mut events) = Outbox::channel();
    let mut store = InMemoryProjectionStore::new(config(), outbox);
    store.enqueue(job(1, 10)).await.unwrap();
    let claimed = store.claim(at(20)).await.unwrap().unwrap();
    assert_eq!(
        *claimed.status(),
        ProjectionStatus::Fitting { started_at: at(20) }
    );
    store
        .complete(projection(1), frame(projection(1), 3, 3, 15), at(30))
        .await
        .unwrap();
    let ready = store.projection(projection(1)).await.unwrap();
    match ready.info().status() {
        ProjectionStatus::Ready(fit) => {
            assert_eq!(fit.started_at, at(20));
            assert_eq!(fit.fitted_at, at(30));
            assert_eq!(fit.watermark, Watermark(at(15)));
            assert_eq!((fit.matching, fit.points), (3, 3));
        }
        other => panic!("expected ready, got {other:?}"),
    }
    assert_eq!(store.expire(at(30 + RETENTION)).await, Ok(0));
    assert_eq!(store.expire(at(31 + RETENTION)).await, Ok(1));
    let info = store.info(projection(1)).await.unwrap().unwrap();
    assert_eq!(status(&info), ProjectionStatusKind::Expired);
    let published = drain(&mut events);
    assert_eq!(
        published,
        vec![
            BusEvent::Changed(Changed::Projection(projection(1))),
            BusEvent::Changed(Changed::Projection(projection(1))),
        ]
    );
}

#[tokio::test]
async fn projection_frames_expire_after_retention() {
    // analysis.projection.frame-retention: an expired projection reads as
    // not retained while its record keeps the fit.
    let mut store = store();
    store.enqueue(job(1, 10)).await.unwrap();
    store.claim(at(20)).await.unwrap();
    store
        .complete(projection(1), frame(projection(1), 2, 2, 15), at(30))
        .await
        .unwrap();
    store.expire(at(5_000)).await.unwrap();
    assert_eq!(
        store.projection(projection(1)).await,
        Err(ProjectionStoreError::NotRetained(projection(1)))
    );
    match store.info(projection(1)).await.unwrap().unwrap().status() {
        ProjectionStatus::Expired { fitted, expired_at } => {
            assert_eq!(fitted.fitted_at, at(30));
            assert_eq!(*expired_at, at(5_000));
        }
        other => panic!("expected expired, got {other:?}"),
    }
}

#[tokio::test]
async fn projection_reads_are_byte_identical() {
    // analysis.projection.read-identical
    let mut store = store();
    store.enqueue(job(1, 10)).await.unwrap();
    store.claim(at(20)).await.unwrap();
    store
        .complete(projection(1), frame(projection(1), 4, 4, 15), at(30))
        .await
        .unwrap();
    let first = store.projection(projection(1)).await.unwrap();
    let second = store.projection(projection(1)).await.unwrap();
    assert_eq!(first.info(), second.info());
    assert_eq!(first.frame().encode(), second.frame().encode());
}

#[tokio::test]
async fn concurrent_enqueues_respect_pending_limit() {
    // analysis.projection.queue-bounded, sequentially
    let mut store = store();
    for n in 0..16 {
        store.enqueue(job(n, 10)).await.unwrap();
    }
    assert_eq!(
        store.enqueue(job(99, 10)).await,
        Err(ProjectionStoreError::QueueFull)
    );
    assert_eq!(store.info(projection(99)).await, Ok(None));
    // Re-enqueuing a known job is a no-op, not QueueFull.
    assert_eq!(store.enqueue(job(3, 10)).await, Ok(()));
    // Finishing one makes room.
    let claimed = store.claim(at(20)).await.unwrap().unwrap();
    store
        .fail(claimed.id(), FitFailure::NonFiniteLayout, at(21))
        .await
        .unwrap();
    assert_eq!(store.pending(), 15);
    assert_eq!(store.enqueue(job(99, 10)).await, Ok(()));
}

#[tokio::test]
async fn enqueue_is_idempotent_on_id() {
    let mut store = store();
    store.enqueue(job(1, 10)).await.unwrap();
    store.claim(at(20)).await.unwrap();
    store.enqueue(job(1, 10)).await.unwrap();
    let info = store.info(projection(1)).await.unwrap().unwrap();
    assert_eq!(status(&info), ProjectionStatusKind::Fitting);
}

#[tokio::test]
async fn fitter_crash_requeues_job() {
    // analysis.projection.crash-requeues
    let mut store = store();
    store.enqueue(job(1, 10)).await.unwrap();
    store.enqueue(job(2, 11)).await.unwrap();
    let first = store.claim(at(20)).await.unwrap().unwrap();
    assert_eq!(first.id(), projection(1), "the oldest job is claimed first");
    // The fitter dies. Before its lease lapses nothing is requeued.
    assert_eq!(store.requeue_lapsed(at(20 + LEASE)).await, Ok(0));
    assert_eq!(store.requeue_lapsed(at(21 + LEASE)).await, Ok(1));
    let info = store.info(projection(1)).await.unwrap().unwrap();
    assert_eq!(*info.status(), ProjectionStatus::Queued);
    // The requeued job keeps its age, so it is claimed again first, and
    // the lapsed fitter's late completion is refused.
    let again = store.claim(at(200)).await.unwrap().unwrap();
    assert_eq!(again.id(), projection(1));
    assert_eq!(
        store
            .complete(projection(1), frame(projection(1), 1, 1, 15), at(201))
            .await,
        Ok(())
    );
    let info = store.info(projection(1)).await.unwrap().unwrap();
    match info.status() {
        ProjectionStatus::Ready(fit) => assert_eq!(fit.started_at, at(200)),
        other => panic!("expected ready, got {other:?}"),
    }
}

#[tokio::test]
async fn complete_needs_a_fitting_job_and_its_frame() {
    let mut store = store();
    store.enqueue(job(1, 10)).await.unwrap();
    assert_eq!(
        store
            .complete(projection(1), frame(projection(1), 1, 1, 5), at(30))
            .await,
        Err(ProjectionJobError::Transition(
            InvalidTransition::NotAllowed {
                from: ProjectionStatusKind::Queued,
                to: ProjectionStatusKind::Ready
            }
        ))
    );
    store.claim(at(20)).await.unwrap();
    // A frame of another job.
    assert_eq!(
        store
            .complete(projection(1), frame(projection(2), 1, 1, 5), at(30))
            .await,
        Err(ProjectionJobError::FrameMismatch {
            projection: projection(1),
            mismatch: ProjectionMismatch::Id
        })
    );
    // A watermark after the start.
    assert!(matches!(
        store
            .complete(projection(1), frame(projection(1), 1, 1, 25), at(30))
            .await,
        Err(ProjectionJobError::Transition(InvalidTransition::Info(_)))
    ));
    // A frame sampled under another limit.
    let mut other_limit = *frame(projection(1), 1, 1, 5).header();
    other_limit.limit = ProjectionLimit::new(5).unwrap();
    let points: Vec<ProjectedPoint> = frame(projection(1), 1, 1, 5).points().collect();
    let other_limit = ProjectionFrame::from_points(other_limit, &points).unwrap();
    assert_eq!(
        store.complete(projection(1), other_limit, at(30)).await,
        Err(ProjectionJobError::FrameMismatch {
            projection: projection(1),
            mismatch: ProjectionMismatch::Limit
        })
    );
    assert_eq!(
        status(&store.info(projection(1)).await.unwrap().unwrap()),
        ProjectionStatusKind::Fitting
    );
    assert_eq!(
        store
            .complete(projection(9), frame(projection(9), 1, 1, 5), at(30))
            .await,
        Err(ProjectionJobError::Unknown(projection(9)))
    );
}

#[tokio::test]
async fn fail_records_whether_the_job_started() {
    let mut store = store();
    store.enqueue(job(1, 10)).await.unwrap();
    store.enqueue(job(2, 11)).await.unwrap();
    store
        .fail(
            projection(2),
            FitFailure::VersionNotRetained {
                version: TopicModelVersion(0),
            },
            at(12),
        )
        .await
        .unwrap();
    store.claim(at(20)).await.unwrap();
    store
        .fail(
            projection(1),
            FitFailure::TooFewPoints { needed: 3, got: 1 },
            at(21),
        )
        .await
        .unwrap();
    match store.info(projection(2)).await.unwrap().unwrap().status() {
        ProjectionStatus::Failed { started_at, .. } => assert_eq!(*started_at, None),
        other => panic!("{other:?}"),
    }
    match store.info(projection(1)).await.unwrap().unwrap().status() {
        ProjectionStatus::Failed { started_at, .. } => assert_eq!(*started_at, Some(at(20))),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        store.projection(projection(1)).await,
        Err(ProjectionStoreError::Failed {
            projection: projection(1),
            failure: FitFailure::TooFewPoints { needed: 3, got: 1 }
        })
    );
    // A failed job never leaves Failed.
    assert!(
        store
            .fail(projection(1), FitFailure::NonFiniteLayout, at(22))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn projection_reports_unknown_and_not_ready() {
    let mut store = store();
    assert_eq!(
        store.projection(projection(1)).await,
        Err(ProjectionStoreError::Unknown(projection(1)))
    );
    store.enqueue(job(1, 10)).await.unwrap();
    assert_eq!(
        store.projection(projection(1)).await,
        Err(ProjectionStoreError::NotReady {
            projection: projection(1),
            status: ProjectionStatusKind::Queued
        })
    );
    // A claim timed before the request is refused, and the job stays
    // queued.
    assert_eq!(
        store.claim(at(5)).await,
        Err(ProjectionJobError::Transition(InvalidTransition::Info(
            InvalidProjectionInfo::TimestampsOutOfOrder
        )))
    );
    assert_eq!(
        status(&store.info(projection(1)).await.unwrap().unwrap()),
        ProjectionStatusKind::Queued
    );
}

#[tokio::test]
async fn list_pages_newest_id_first() {
    let mut store = store();
    for n in 1..=5 {
        store.enqueue(job(n, 10)).await.unwrap();
    }
    let mut request: PageRequest<ProjectionList> = PageRequest {
        size: PageSize::new(2).unwrap(),
        after: None,
    };
    let mut seen = Vec::new();
    loop {
        let page = store.list(&request).await.unwrap();
        let (items, next) = page.into_parts();
        seen.extend(items.iter().map(ProjectionInfo::id));
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => break,
        }
    }
    assert_eq!(seen, (1..=5).rev().map(projection).collect::<Vec<_>>());
    assert_eq!(
        store.claim(at(20)).await.unwrap().map(|job| job.id()),
        Some(projection(1))
    );
}

#[tokio::test]
async fn claim_on_an_empty_queue_is_none() {
    let mut store = store();
    assert_eq!(store.claim(at(1)).await, Ok(None));
    assert_eq!(store.requeue_lapsed(at(1)).await, Ok(0));
    assert_eq!(store.expire(at(1)).await, Ok(0));
}
