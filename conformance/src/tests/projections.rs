//! Projection jobs: each fit records a job that runs to a stored frame
//! (in the background: tests wait for it), samples follow the window,
//! filter and seed, and a ready frame reads back identically.

use std::collections::HashSet;
use std::time::Duration;

use crosstalk_spec::aggregates::edge::{RouteKind, TopologyFilter};
use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::aggregates::projection::{
    FitFailure, Projection, ProjectionInfo, ProjectionLimit, ProjectionParams, ProjectionStatus,
    ProjectionStatusKind,
};
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l8_surface::summary::TopicUnder;
use crosstalk_spec::interfaces::l8_surface::{ConflictKind, QueryApi, QueryError};
use crosstalk_spec::support::TimeWindow;

use crate::harness::Harness;
use crate::support::World;
use crate::support::reads::{current_version, rows};
use crate::support::windows::quiet;

/// Fifteen neighbours, a minimum distance of 0.1, `limit` points.
pub fn params(seed: u64, limit: u32) -> ProjectionParams {
    ProjectionLimit::new(limit)
        .ok()
        .and_then(|limit| ProjectionParams::new(limit, 15, 100, seed).ok())
        .unwrap_or_else(|| panic!("projection params {seed} {limit}"))
}

/// The job once it has left the queue: ready or failed. A fit runs in the
/// background, so this polls `projection_status` for up to a minute.
pub async fn settled<H: Harness>(w: &World<'_, H>, id: ProjectionId) -> ProjectionInfo {
    for _ in 0..6_000 {
        let info = w
            .backend
            .projection_status(&w.lead, id)
            .await
            .unwrap_or_else(|e| panic!("status of {id:?}: {e:?}"));
        match info.status().kind() {
            ProjectionStatusKind::Queued | ProjectionStatusKind::Fitting => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            ProjectionStatusKind::Ready
            | ProjectionStatusKind::Failed
            | ProjectionStatusKind::Expired => return info,
        }
    }
    panic!("{id:?} never left the queue")
}

/// Fits `window` under `filter` and waits for the stored projection.
async fn fit<H: Harness>(
    w: &World<'_, H>,
    window: TimeWindow,
    filter: &TopologyFilter,
    params: ProjectionParams,
) -> Projection {
    let id = w
        .backend
        .fit_projection(&w.lead, window, filter, params)
        .await
        .unwrap_or_else(|e| panic!("fit: {e:?}"));
    let info = settled(w, id).await;
    assert_eq!(info.status().kind(), ProjectionStatusKind::Ready, "{:?}", info.status());
    w.backend
        .projection(&w.lead, id)
        .await
        .unwrap_or_else(|e| panic!("projection {id:?}: {e:?}"))
}

/// Each call records a new job (INV-639) whose spec pins the resolved
/// version and records the window, caller and parameters; the same request
/// samples and lays out the same frame (INV-623), another seed another
/// sample; the frame reads back identically (INV-632) under the watermark
/// its sample was read at (INV-634); at most `limit` points (INV-393).
pub async fn every_fit_is_a_new_job_with_a_reproducible_frame<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let f = TopologyFilter::default();
    let version = current_version(&w.backend, &w.lead, w.extent).await;
    let first = fit(&w, w.extent, &f, params(42, 300)).await;
    let again = fit(&w, w.extent, &f, params(42, 300)).await;
    assert_ne!(first.info().id(), again.info().id(), "each call records a new job");
    assert_eq!(first.frame().columns(), again.frame().columns());
    assert_eq!(first.frame().tables(), again.frame().tables());
    let info = first.info();
    assert_eq!(info.requested_by(), w.lead.operator());
    let spec = info.spec();
    assert_eq!(spec.topic_version(), version);
    assert_eq!(spec.filter().topic_version, TopicVersionSelector::Pinned(version));
    assert_eq!(spec.window(), w.extent);
    assert_eq!(spec.params(), params(42, 300));
    let ProjectionStatus::Ready(fitted) = info.status() else {
        panic!("ready")
    };
    assert_eq!(first.watermark(), fitted.watermark);
    assert!(fitted.points <= 300);
    assert_eq!(u64::from(fitted.points), fitted.matching.min(300));
    assert_eq!(first.frame().count(), fitted.points);
    let reread = w
        .backend
        .projection(&w.lead, info.id())
        .await
        .expect("the same frame");
    assert_eq!(reread, first);
    if first.is_sampled() {
        let other = fit(&w, w.extent, &f, params(43, 300)).await;
        assert_ne!(
            first.frame().columns().transmissions,
            other.frame().columns().transmissions,
            "another seed samples otherwise"
        );
    }
}

/// Every point is a transmission the window and filter admit, with its
/// topic under the pinned version (INV-394, INV-381, INV-395); below the
/// limit nothing is sampled away.
pub async fn samples_honour_the_window_and_filter<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let filter = TopologyFilter {
        route_kinds: vec![RouteKind::Channel],
        ..TopologyFilter::default()
    };
    let window = w.day();
    let projection = fit(&w, window, &filter, params(1, 100_000)).await;
    assert!(!projection.is_sampled(), "below the limit everything is kept");
    let points: Vec<_> = projection.frame().points().collect();
    assert!(!points.is_empty());
    let ids: Vec<_> = points.iter().map(|p| p.transmission).collect();
    let version = projection.topic_version();
    let summaries = rows(&w.backend, &w.lead, &ids, TopicVersionSelector::Pinned(version)).await;
    for point in &points {
        assert_eq!(point.route, RouteKind::Channel);
        assert!(window.contains(point.confirmed_at));
        assert_ne!(point.from, point.to, "a transmission between two agents (INV-758)");
        let summary = summaries
            .iter()
            .find(|s| s.id == point.transmission)
            .expect("a stored transmission");
        let topic = match summary.state.topic() {
            Some(TopicUnder::Topic(t)) => Some(t),
            _ => None,
        };
        assert_eq!(point.topic, topic, "{:?}", point.transmission);
    }
}

/// With the same seed, a narrower window keeps every point of the wider
/// sample that it admits (INV-396).
pub async fn a_narrower_fit_keeps_what_it_admits_of_a_wider_sample<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let f = TopologyFilter::default();
    let wide = fit(&w, w.extent, &f, params(42, 300)).await;
    let narrow = fit(&w, w.day(), &f, params(42, 300)).await;
    let kept: HashSet<_> = narrow.frame().columns().transmissions.iter().copied().collect();
    let expected: Vec<_> = wide
        .frame()
        .points()
        .filter(|p| w.day().contains(p.confirmed_at))
        .map(|p| p.transmission)
        .collect();
    assert!(expected.iter().all(|id| kept.contains(id)));
}

/// A window with too few points fails its job, and reading it is
/// `Conflict(ProjectionFailed)` (INV-629).
pub async fn too_few_points_fail_the_job<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let id = w
        .backend
        .fit_projection(&w.lead, quiet(w.bucket), &TopologyFilter::default(), params(1, 100))
        .await
        .expect("recorded");
    let info = settled(&w, id).await;
    let ProjectionStatus::Failed { failure, .. } = info.status() else {
        panic!("failed: {:?}", info.status())
    };
    assert!(matches!(failure, FitFailure::TooFewPoints { got: 0, .. }), "{failure:?}");
    assert!(matches!(
        w.backend.projection(&w.lead, id).await,
        Err(QueryError::Conflict(ConflictKind::ProjectionFailed { projection, .. })) if projection == id
    ));
}

/// Jobs are listed newest first, the fits just recorded among them.
pub async fn jobs_list_newest_first<H: Harness>(h: &H) {
    let w = World::everything(h).await;
    let id = w
        .backend
        .fit_projection(&w.lead, w.day(), &TopologyFilter::default(), params(7, 50))
        .await
        .expect("fit");
    settled(&w, id).await;
    let jobs = crate::support::collect(5, async |p| w.backend.projections(&w.lead, &p).await).await;
    assert!(jobs.windows(2).all(|p| p[0].id() > p[1].id()), "newest first");
    assert!(jobs.iter().any(|j| j.id() == id));
}
