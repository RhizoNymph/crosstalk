//! Projection jobs: fitting records a job and runs it through the spec's
//! lifecycle, samples follow the spec's selection, and reads fail as the
//! projection store does.

use std::collections::HashSet;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::{
    FitFailure, Projection, ProjectionInfo, ProjectionStatus, ProjectionStatusKind,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryError};
use crosstalk_spec::support::TimeWindow;

use super::super::FixtureBackend;
use super::super::clock::{MINUTE, START, WATERMARK, plus};
use super::super::queries::projection::{FRAME_RETENTION, MAX_PENDING};
use super::super::store::Job;
use super::scope::{Scope, ViewFilter};
use super::{collect, day, first, fresh, researcher, shared, week};
use crosstalk_spec::interfaces::l8_surface::QueryApi;

use super::reads_support::*;

async fn fit(b: &FixtureBackend, scope: &Scope, seed: u64, limit: u32) -> Projection {
    let c = researcher();
    let id = b
        .fit_projection(
            &c,
            scope.window,
            &scope.topology_filter(),
            params(seed, limit),
        )
        .await
        .expect("fit");
    b.projection(&c, id).await.expect("ready")
}

fn ids(projection: &Projection) -> Vec<crosstalk_spec::ids::TransmissionId> {
    projection.frame().columns().transmissions.clone()
}

#[tokio::test]
async fn every_fit_is_a_new_ready_job_with_a_reproducible_frame() {
    let a = fresh();
    let b = fresh();
    let scope = week();
    let first_fit = fit(&a, &scope, 42, 300).await;
    let again = fit(&a, &scope, 42, 300).await;
    assert_ne!(
        first_fit.info().id(),
        again.info().id(),
        "each call records a new job"
    );
    assert_eq!(first_fit.frame().columns(), again.frame().columns());
    assert_eq!(first_fit.frame().tables(), again.frame().tables());
    let elsewhere = fit(&b, &scope, 42, 300).await;
    assert_eq!(first_fit.frame().columns(), elsewhere.frame().columns());

    let info = first_fit.info();
    assert_eq!(info.requested_by(), researcher().operator());
    let ProjectionStatus::Ready(fitted) = info.status() else {
        panic!("ready")
    };
    assert_eq!(fitted.watermark.at(), WATERMARK);
    assert_eq!(first_fit.watermark(), fitted.watermark);
    assert_eq!(fitted.points, 300);
    assert!(fitted.matching > 300);
    assert!(first_fit.is_sampled());
    let spec = info.spec();
    assert_eq!(spec.topic_version(), TopicModelVersion(2));
    assert_eq!(
        spec.filter().topic_version,
        TopicVersionSelector::Pinned(TopicModelVersion(2))
    );
    assert_eq!(spec.window(), scope.window);
    assert_eq!(*spec.embedding_model(), a.world.topics.model);

    let other_seed = fit(&a, &scope, 43, 300).await;
    assert_ne!(ids(&first_fit), ids(&other_seed));
    let points: Vec<_> = first_fit.frame().points().collect();
    assert!(points.iter().any(|p| p.topic.is_some()));
    assert!(points.iter().any(|p| p.topic.is_none()));
    assert!(points.iter().any(|p| p.route == RouteKind::Channel));
}

#[tokio::test]
async fn samples_honour_the_window_and_filter() {
    let b = shared();
    let scope = with(ViewFilter {
        route_kinds: vec![RouteKind::Channel],
        ..ViewFilter::default()
    });
    let projection = fit(b, &scope, 1, 100_000).await;
    assert!(
        !projection.is_sampled(),
        "below the limit everything is kept"
    );
    for point in projection.frame().points() {
        assert_eq!(point.route, RouteKind::Channel);
        assert!(scope.window.contains(point.confirmed_at));
        let record = b.world.tx(point.transmission).expect("stored");
        assert_eq!(record.topic(TopicModelVersion(2)), point.topic);
    }
}

#[tokio::test]
async fn a_narrower_fit_keeps_what_it_admits_of_a_wider_sample() {
    let b = shared();
    let wide = fit(b, &week(), 42, 300).await;
    let narrow_scope = day();
    let narrow = fit(b, &narrow_scope, 42, 300).await;
    let kept: HashSet<_> = ids(&narrow).into_iter().collect();
    let expected: Vec<_> = wide
        .frame()
        .points()
        .filter(|p| narrow_scope.window.contains(p.confirmed_at))
        .map(|p| p.transmission)
        .collect();
    assert!(!expected.is_empty());
    assert!(expected.iter().all(|id| kept.contains(id)));
}

#[tokio::test]
async fn too_few_points_fail_the_job() {
    let b = fresh();
    let c = researcher();
    let empty = TimeWindow::new(START, plus(START, 5 * MINUTE)).expect("window");
    let filter = week().topology_filter();
    let id = b
        .fit_projection(&c, empty, &filter, params(1, 100))
        .await
        .expect("recorded");
    let info = b.projection_status(&c, id).await.expect("status");
    let ProjectionStatus::Failed {
        started_at,
        failure,
        ..
    } = info.status()
    else {
        panic!("failed: {:?}", info.status())
    };
    assert!(started_at.is_some());
    assert!(matches!(
        failure,
        FitFailure::TooFewPoints { needed: 16, got } if *got <= 15
    ));
    assert!(matches!(
        b.projection(&c, id).await,
        Err(QueryError::Conflict(ConflictKind::ProjectionFailed { projection, .. })) if projection == id
    ));
}

#[tokio::test]
async fn fits_resolve_the_filters_version_as_a_linked_view() {
    let b = shared();
    let c = researcher();
    let dropped = Scope {
        topic_version: TopicModelVersion(0),
        ..week()
    };
    assert_eq!(
        b.fit_projection(
            &c,
            dropped.window,
            &dropped.topology_filter(),
            params(1, 10)
        )
        .await
        .err(),
        Some(QueryError::VersionNotRetained {
            version: TopicModelVersion(0)
        })
    );
}

/// The seeded job in `status`.
async fn seeded(status: ProjectionStatusKind) -> ProjectionInfo {
    let b = shared();
    collect(2, async |page| b.projections(&researcher(), &page).await)
        .await
        .into_iter()
        .find(|info| info.status().kind() == status)
        .expect("a seeded job")
}

#[tokio::test]
async fn seeded_jobs_cover_every_other_status_and_read_as_the_store_fails() {
    let b = shared();
    let c = researcher();
    let jobs = collect(2, async |page| b.projections(&c, &page).await).await;
    assert!(
        jobs.windows(2).all(|pair| pair[0].id() > pair[1].id()),
        "newest first"
    );
    let not_ready = |status| {
        move |error: QueryError| {
            matches!(
                error,
                QueryError::Conflict(ConflictKind::ProjectionNotReady { status: s, .. }) if s == status
            )
        }
    };
    for status in [ProjectionStatusKind::Queued, ProjectionStatusKind::Fitting] {
        let job = seeded(status).await;
        let error = b.projection(&c, job.id()).await.expect_err("not ready");
        assert!(not_ready(status)(error));
    }
    let failed = seeded(ProjectionStatusKind::Failed).await;
    assert!(matches!(
        failed.status(),
        ProjectionStatus::Failed {
            started_at: None,
            failure: FitFailure::VersionNotRetained { version },
            ..
        } if *version == TopicModelVersion(0)
    ));
    let expired = seeded(ProjectionStatusKind::Expired).await;
    assert_eq!(
        b.projection(&c, expired.id()).await.err(),
        Some(QueryError::ProjectionNotRetained {
            projection: expired.id()
        })
    );
    let ProjectionStatus::Expired { fitted, expired_at } = expired.status() else {
        panic!("expired")
    };
    assert_eq!(*expired_at, plus(fitted.fitted_at, FRAME_RETENTION));
    assert!(fitted.points > 0, "its spec and fit record stay readable");
    assert_eq!(
        b.projection_status(&c, ProjectionId::from_ulid(5))
            .await
            .err(),
        Some(QueryError::NotFound)
    );
    assert_eq!(
        b.projection(&c, ProjectionId::from_ulid(5)).await.err(),
        Some(QueryError::NotFound)
    );
}

#[tokio::test]
async fn a_full_queue_refuses_new_jobs() {
    let b = fresh();
    let c = researcher();
    {
        let mut state = b.state.write().await;
        let queued = state
            .projections
            .iter()
            .find(|job| job.info().status().kind() == ProjectionStatusKind::Queued)
            .map(|job| job.info().clone())
            .expect("a queued job");
        while state
            .projections
            .iter()
            .filter(|job| {
                matches!(
                    job.info().status().kind(),
                    ProjectionStatusKind::Queued | ProjectionStatusKind::Fitting
                )
            })
            .count()
            < MAX_PENDING
        {
            let id = ProjectionId::from_ulid(state.mint.ulid(queued.requested_at()));
            let copy = ProjectionInfo::queued(
                id,
                queued.spec().clone(),
                queued.requested_by(),
                queued.requested_at(),
            );
            state.projections.push(Job::record(copy));
        }
    }
    let scope = week();
    assert_eq!(
        b.fit_projection(&c, scope.window, &scope.topology_filter(), params(1, 10))
            .await
            .err(),
        Some(QueryError::Conflict(ConflictKind::ProjectionQueueFull))
    );
    assert!(
        b.projections(&c, &first(50))
            .await
            .expect("jobs")
            .items()
            .len()
            >= MAX_PENDING
    );
}
