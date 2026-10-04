use std::num::NonZeroU16;

use crate::aggregates::edge::{TopicSlot, TopologyFilter};
use crate::aggregates::filter::TopicVersionSelector;
use crate::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crate::aggregates::projection::{
    FitFailure, Fitted, InvalidParams, InvalidProjectionInfo, InvalidProjectionLimit,
    InvalidTransition, PointParts, PointRoute, ProjectedPoint, Projection, ProjectionInfo,
    ProjectionLimit, ProjectionMismatch, ProjectionParams, ProjectionSpec, ProjectionStatus,
    ProjectionStatusKind,
};
use crate::aggregates::topic::{EmbeddingModel, TopicModelVersion};
use crate::ids::{OperatorId, ProjectionId, TopicId};
use crate::support::{Finite, TimeWindow, Watermark};
use crate::tests::fixtures::{agent, at, transmission};

const VERSION: TopicModelVersion = TopicModelVersion(3);

fn limit(n: u32) -> ProjectionLimit {
    ProjectionLimit::new(n).expect("fixture limits are in range")
}

fn params(sample: u32) -> ProjectionParams {
    ProjectionParams::new(
        limit(sample),
        ProjectionParams::DEFAULT_NEIGHBORS,
        ProjectionParams::DEFAULT_MIN_DIST_MILLI,
        42,
    )
    .expect("default params are valid")
}

fn model() -> EmbeddingModel {
    EmbeddingModel {
        name: "test".into(),
        dimension: NonZeroU16::new(4).expect("non-zero"),
    }
}

fn spec(sample: u32) -> ProjectionSpec {
    let window = TimeWindow::new(at(0), at(1_000)).expect("non-empty");
    ProjectionSpec::new(
        window,
        TopologyFilter::default(),
        VERSION,
        params(sample),
        model(),
    )
}

fn id() -> ProjectionId {
    ProjectionId::from_ulid(77)
}

fn operator() -> OperatorId {
    OperatorId::from_ulid(5)
}

/// Requested at 10.
fn queued(sample: u32) -> ProjectionInfo {
    ProjectionInfo::queued(id(), spec(sample), operator(), at(10))
}

/// Started at 20 with watermark 15, fitted at 30.
fn fitted(matching: u64, points: u32) -> Fitted {
    Fitted {
        started_at: at(20),
        fitted_at: at(30),
        watermark: Watermark(at(15)),
        matching,
        points,
    }
}

fn info(sample: u32, status: ProjectionStatus) -> Result<ProjectionInfo, InvalidProjectionInfo> {
    ProjectionInfo::new(id(), spec(sample), operator(), at(10), status)
}

fn point(n: u128) -> ProjectedPoint {
    ProjectedPoint::new(PointParts {
        transmission: transmission(n),
        from: agent(1),
        to: agent(2),
        route: PointRoute::Unobserved,
        topic: Some(TopicId::from_ulid(7)),
        confirmed_at: at(10),
        x: Finite::new(0.5).expect("finite"),
        y: Finite::new(-1.5).expect("finite"),
    })
    .expect("a point between two agents")
}

fn header(sample: u32, matching: u64) -> FrameHeader {
    FrameHeader {
        projection: id(),
        topic_version: VERSION,
        watermark: Watermark(at(15)),
        limit: limit(sample),
        matching,
    }
}

fn frame(sample: u32, matching: u64, points: &[ProjectedPoint]) -> ProjectionFrame {
    ProjectionFrame::from_points(header(sample, matching), points).expect("valid frame")
}

// ── Limits and params ──────────────────────────────────────────────────────

#[test]
fn projection_limit_rejects_zero_and_above_max() {
    assert_eq!(ProjectionLimit::new(0), Err(InvalidProjectionLimit::Zero));
    assert_eq!(
        ProjectionLimit::new(ProjectionLimit::MAX + 1),
        Err(InvalidProjectionLimit::AboveMax {
            max: ProjectionLimit::MAX,
            got: ProjectionLimit::MAX + 1
        })
    );
    assert_eq!(
        limit(ProjectionLimit::MAX).get().get(),
        ProjectionLimit::MAX
    );
}

#[test]
fn params_reject_neighbors_outside_range() {
    for neighbors in [0, 1, ProjectionParams::MAX_NEIGHBORS + 1] {
        assert_eq!(
            ProjectionParams::new(limit(10), neighbors, 100, 0),
            Err(InvalidParams::Neighbors {
                min: ProjectionParams::MIN_NEIGHBORS,
                max: ProjectionParams::MAX_NEIGHBORS,
                got: neighbors
            })
        );
    }
    for neighbors in [
        ProjectionParams::MIN_NEIGHBORS,
        ProjectionParams::MAX_NEIGHBORS,
    ] {
        assert!(ProjectionParams::new(limit(10), neighbors, 100, 0).is_ok());
    }
}

#[test]
fn params_reject_min_dist_above_one() {
    assert_eq!(
        ProjectionParams::new(limit(10), 15, 1_001, 0),
        Err(InvalidParams::MinDist {
            max_milli: 1_000,
            got_milli: 1_001
        })
    );
    let edge = ProjectionParams::new(limit(10), 15, 1_000, 0).expect("1.0 is allowed");
    assert!((edge.min_dist() - 1.0).abs() < f32::EPSILON);
}

#[test]
fn params_keep_every_field() {
    let params = ProjectionParams::new(limit(9), 30, 250, 7).expect("valid");
    assert_eq!(params.limit(), limit(9));
    assert_eq!(params.neighbors(), 30);
    assert_eq!(params.min_dist_milli(), 250);
    assert!((params.min_dist() - 0.25).abs() < f32::EPSILON);
    assert_eq!(params.seed(), 7);
}

// ── Spec ───────────────────────────────────────────────────────────────────

#[test]
fn spec_pins_the_filter_to_the_resolved_version() {
    let window = TimeWindow::new(at(0), at(1)).expect("non-empty");
    for selector in [
        TopicVersionSelector::Current,
        TopicVersionSelector::Pinned(TopicModelVersion(9)),
    ] {
        let filter = TopologyFilter {
            topic_version: selector,
            ..TopologyFilter::default()
        };
        let spec = ProjectionSpec::new(window, filter, VERSION, params(5), model());
        assert_eq!(spec.topic_version(), VERSION);
        assert_eq!(
            spec.filter().topic_version,
            TopicVersionSelector::Pinned(VERSION)
        );
    }
}

// ── Job record ─────────────────────────────────────────────────────────────

#[test]
fn info_accepts_every_status_with_ordered_times() {
    let statuses = [
        ProjectionStatus::Queued,
        ProjectionStatus::Fitting { started_at: at(20) },
        ProjectionStatus::Ready(fitted(5, 5)),
        ProjectionStatus::Failed {
            started_at: None,
            failed_at: at(11),
            failure: FitFailure::VersionNotRetained { version: VERSION },
        },
        ProjectionStatus::Expired {
            fitted: fitted(5, 5),
            expired_at: at(40),
        },
    ];
    for status in statuses {
        assert!(info(10, status.clone()).is_ok(), "{status:?}");
    }
}

#[test]
fn info_rejects_times_out_of_order() {
    let early_start = ProjectionStatus::Fitting { started_at: at(9) };
    let early_expiry = ProjectionStatus::Expired {
        fitted: fitted(5, 5),
        expired_at: at(29),
    };
    let fail_before_start = ProjectionStatus::Failed {
        started_at: Some(at(20)),
        failed_at: at(19),
        failure: FitFailure::NonFiniteLayout,
    };
    for status in [early_start, early_expiry, fail_before_start] {
        assert_eq!(
            info(10, status),
            Err(InvalidProjectionInfo::TimestampsOutOfOrder)
        );
    }
}

#[test]
fn info_rejects_watermark_after_start() {
    let late = Fitted {
        watermark: Watermark(at(21)),
        ..fitted(5, 5)
    };
    assert_eq!(
        info(10, ProjectionStatus::Ready(late)),
        Err(InvalidProjectionInfo::WatermarkAfterStart)
    );
}

#[test]
fn info_rejects_a_count_other_than_min_of_matching_and_limit() {
    // Sampled: 8 match, sample size 3, so 3 points.
    assert!(info(3, ProjectionStatus::Ready(fitted(8, 3))).is_ok());
    assert_eq!(
        info(3, ProjectionStatus::Ready(fitted(8, 2))),
        Err(InvalidProjectionInfo::WrongCount {
            expected: 3,
            got: 2
        })
    );
    // Not sampled: 2 match.
    assert_eq!(
        info(3, ProjectionStatus::Ready(fitted(2, 3))),
        Err(InvalidProjectionInfo::WrongCount {
            expected: 2,
            got: 3
        })
    );
}

#[test]
fn job_runs_queued_fitting_ready_expired() {
    let job = queued(10).start(at(20)).expect("queued starts");
    assert_eq!(
        job.status(),
        &ProjectionStatus::Fitting { started_at: at(20) }
    );
    let job = job.complete(fitted(4, 4)).expect("fitting completes");
    assert_eq!(job.status(), &ProjectionStatus::Ready(fitted(4, 4)));
    let job = job.expire(at(50)).expect("ready expires");
    assert_eq!(job.status().kind(), ProjectionStatusKind::Expired);
    assert_eq!(job.requested_at(), at(10));
    assert_eq!(job.requested_by(), operator());
}

#[test]
fn lapsed_fit_requeues_and_restarts() {
    let job = queued(10).start(at(20)).expect("starts");
    let job = job.requeue().expect("fitting requeues");
    assert_eq!(job.status(), &ProjectionStatus::Queued);
    assert!(job.start(at(25)).is_ok());
}

#[test]
fn complete_needs_the_jobs_start() {
    let job = queued(10).start(at(19)).expect("starts");
    assert_eq!(
        job.complete(fitted(4, 4)),
        Err(InvalidTransition::StartMismatch)
    );
}

#[test]
fn complete_checks_the_fit() {
    let job = queued(3).start(at(20)).expect("starts");
    assert_eq!(
        job.complete(fitted(8, 2)),
        Err(InvalidTransition::Info(InvalidProjectionInfo::WrongCount {
            expected: 3,
            got: 2
        }))
    );
}

#[test]
fn fail_records_whether_the_job_started() {
    let failure = FitFailure::TooFewPoints { needed: 16, got: 3 };
    let before = queued(10)
        .fail(at(12), failure.clone())
        .expect("queued fails");
    assert_eq!(
        before.status(),
        &ProjectionStatus::Failed {
            started_at: None,
            failed_at: at(12),
            failure: failure.clone(),
        }
    );
    let during = queued(10)
        .start(at(20))
        .and_then(|job| job.fail(at(21), failure.clone()))
        .expect("fitting fails");
    assert_eq!(
        during.status(),
        &ProjectionStatus::Failed {
            started_at: Some(at(20)),
            failed_at: at(21),
            failure,
        }
    );
}

#[test]
fn transitions_outside_the_lifecycle_are_refused() {
    let ready = queued(10)
        .start(at(20))
        .and_then(|job| job.complete(fitted(4, 4)))
        .expect("ready");
    let not_allowed = |from, to| InvalidTransition::NotAllowed { from, to };
    assert_eq!(
        ready.clone().start(at(40)),
        Err(not_allowed(
            ProjectionStatusKind::Ready,
            ProjectionStatusKind::Fitting
        ))
    );
    assert_eq!(
        ready.clone().fail(at(40), FitFailure::NonFiniteLayout),
        Err(not_allowed(
            ProjectionStatusKind::Ready,
            ProjectionStatusKind::Failed
        ))
    );
    assert_eq!(
        ready.requeue(),
        Err(not_allowed(
            ProjectionStatusKind::Ready,
            ProjectionStatusKind::Queued
        ))
    );
    assert_eq!(
        queued(10).expire(at(40)),
        Err(not_allowed(
            ProjectionStatusKind::Queued,
            ProjectionStatusKind::Expired
        ))
    );
    assert_eq!(
        queued(10).complete(fitted(4, 4)),
        Err(not_allowed(
            ProjectionStatusKind::Queued,
            ProjectionStatusKind::Ready
        ))
    );
}

// ── Ready projection ───────────────────────────────────────────────────────

fn ready(sample: u32, matching: u64, points: u32) -> ProjectionInfo {
    info(sample, ProjectionStatus::Ready(fitted(matching, points))).expect("ready info")
}

#[test]
fn projection_joins_a_ready_job_and_its_frame() {
    let points = [point(1), point(2)];
    let projection =
        Projection::new(ready(2, 5, 2), frame(2, 5, &points)).expect("frame matches job");
    assert!(projection.is_sampled());
    assert_eq!(projection.topic_version(), VERSION);
    assert_eq!(projection.watermark(), Watermark(at(15)));
    assert_eq!(projection.frame().points().collect::<Vec<_>>(), points);
    assert_eq!(
        projection.slot(&points[0]),
        TopicSlot {
            version: VERSION,
            topic: Some(TopicId::from_ulid(7)),
        }
    );
}

#[test]
fn projection_needs_a_ready_job() {
    assert_eq!(
        Projection::new(queued(2), frame(2, 2, &[point(1), point(2)])),
        Err(ProjectionMismatch::NotReady(ProjectionStatusKind::Queued))
    );
}

#[test]
fn projection_rejects_a_frame_from_elsewhere() {
    let points = [point(1), point(2)];
    let job = ready(2, 2, 2);
    let mismatched = |change: fn(&mut FrameHeader)| {
        let mut header = header(2, 2);
        change(&mut header);
        let frame = ProjectionFrame::from_points(header, &points).expect("valid frame");
        Projection::new(job.clone(), frame)
    };
    assert_eq!(
        mismatched(|h| h.projection = ProjectionId::from_ulid(1)),
        Err(ProjectionMismatch::Id)
    );
    assert_eq!(
        mismatched(|h| h.topic_version = TopicModelVersion(4)),
        Err(ProjectionMismatch::TopicVersion)
    );
    assert_eq!(
        mismatched(|h| h.watermark = Watermark(at(14))),
        Err(ProjectionMismatch::Watermark)
    );
    assert_eq!(
        mismatched(|h| h.limit = limit(3)),
        Err(ProjectionMismatch::Limit)
    );
    // Sampled frame (2 of 9) against a job that matched 2.
    let other = ProjectionFrame::from_points(header(2, 9), &points).expect("valid frame");
    assert_eq!(Projection::new(job, other), Err(ProjectionMismatch::Counts));
}

/// A projection holds transmissions between different agents only, so a
/// point whose sender is its reader is refused, built or decoded.
#[test]
fn a_projected_point_is_never_within_one_agent() {
    let within = PointParts {
        from: agent(2),
        ..*point(1).parts()
    };
    assert_eq!(
        ProjectedPoint::new(within),
        Err(crate::aggregates::projection::PointWithinOneAgent(agent(2)))
    );
    let json = serde_json::to_string(&within).expect("parts encode");
    crate::tests::wire::harness::assert_rejected::<ProjectedPoint>(
        &json,
        "invalid projected point: PointWithinOneAgent",
    );
    // The same parts with two agents decode to the point.
    let between = *point(1).parts();
    let decoded: ProjectedPoint =
        serde_json::from_str(&serde_json::to_string(&between).expect("parts encode"))
            .expect("a point between two agents decodes");
    assert_eq!(decoded, point(1));
    assert_eq!((decoded.from(), decoded.to()), (agent(1), agent(2)));
}
