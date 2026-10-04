//! `check_projection_store`: the projection job store against the
//! reference, with the queue bound (`analysis.projection.queue-bounded`)
//! checked after every operation.

use std::time::Duration;

use proptest::prelude::*;

use crosstalk_spec::aggregates::edge::RouteKind;
use crosstalk_spec::aggregates::filter::TopologyFilter;
use crosstalk_spec::aggregates::projection::frame::{FrameHeader, ProjectionFrame};
use crosstalk_spec::aggregates::projection::{
    FitFailure, ProjectedPoint, ProjectionInfo, ProjectionLimit, ProjectionParams, ProjectionSpec,
    ProjectionStatusKind,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::watermark::Watermark;
use crosstalk_spec::ids::ProjectionId;
use crosstalk_spec::interfaces::l6_analysis::{ProjectionStore, ProjectionStoreError};
use crosstalk_spec::paging::{PageRequest, PageSize, ProjectionList};
use crosstalk_spec::support::Finite;

use super::search::harness_model;
use crate::analysis::projection::{InMemoryProjectionStore, ProjectionConfig};
use crate::model::build::{agent, operator, projection, transmission, ts, window};
use crate::model::{Divergence, HarnessConfig, ModelMismatch, holds, run, same};

/// The harness's store configuration: 50 µs leases, 500 µs frame
/// retention.
pub fn projection_config() -> ProjectionConfig {
    ProjectionConfig {
        lease: Duration::from_micros(50),
        frame_retention: Duration::from_micros(500),
    }
}

/// The sample size every harness job asks for.
const LIMIT: u32 = 5;

#[derive(Debug, Clone)]
pub enum ProjectionOp {
    Enqueue {
        job: u64,
    },
    Claim,
    Complete {
        job: u64,
        matching: u64,
        watermark_back: u64,
        wrong_id: bool,
    },
    Fail {
        job: u64,
    },
    RequeueLapsed,
    Expire,
    /// Let time pass.
    Wait {
        micros: u64,
    },
}

fn projection_op() -> impl Strategy<Value = ProjectionOp> {
    prop_oneof![
        4 => (0u64..20).prop_map(|job| ProjectionOp::Enqueue { job }),
        3 => Just(ProjectionOp::Claim),
        3 => (0u64..20, 0u64..9, 0u64..30, prop::bool::weighted(0.1))
            .prop_map(|(job, matching, watermark_back, wrong_id)| ProjectionOp::Complete { job, matching, watermark_back, wrong_id }),
        1 => (0u64..20).prop_map(|job| ProjectionOp::Fail { job }),
        1 => Just(ProjectionOp::RequeueLapsed),
        1 => Just(ProjectionOp::Expire),
        2 => (1u64..300).prop_map(|micros| ProjectionOp::Wait { micros }),
    ]
}

fn spec() -> Option<ProjectionSpec> {
    let params = ProjectionParams::new(ProjectionLimit::new(LIMIT).ok()?, 2, 100, 3).ok()?;
    Some(ProjectionSpec::new(
        window(0, 1_000)?,
        TopologyFilter::default(),
        TopicModelVersion(0),
        params,
        harness_model(),
    ))
}

/// A frame for `id` from `matching` admitted transmissions, sampled to the
/// limit, read at `watermark`.
fn frame(id: ProjectionId, matching: u64, watermark: u64) -> Option<ProjectionFrame> {
    let header = FrameHeader {
        projection: id,
        topic_version: TopicModelVersion(0),
        watermark: Watermark(ts(watermark)),
        limit: ProjectionLimit::new(LIMIT).ok()?,
        matching,
    };
    let points: Vec<ProjectedPoint> = (0..matching.min(u64::from(LIMIT)))
        .map(|n| {
            Some(ProjectedPoint {
                transmission: transmission(n),
                from: agent(1),
                to: agent(2),
                route: RouteKind::Unobserved,
                topic: None,
                confirmed_at: ts(n),
                x: Finite::new(0.25).ok()?,
                y: Finite::new(0.75).ok()?,
            })
        })
        .collect::<Option<_>>()?;
    ProjectionFrame::from_points(header, &points).ok()
}

/// Every job and every frame, as compared.
#[derive(Debug, PartialEq)]
struct StoreView {
    listed: Result<Vec<ProjectionInfo>, ProjectionStoreError>,
    infos: Vec<Result<Option<ProjectionInfo>, ProjectionStoreError>>,
    frames: Vec<Result<Vec<u8>, ProjectionStoreError>>,
}

async fn view<S: ProjectionStore>(store: &S) -> StoreView {
    let mut infos = Vec::new();
    let mut frames = Vec::new();
    for job in 0..20 {
        infos.push(store.info(projection(job)).await);
        frames.push(
            store
                .projection(projection(job))
                .await
                .map(|ready| ready.frame().encode()),
        );
    }
    StoreView {
        listed: list(store).await,
        infos,
        frames,
    }
}

async fn list<S: ProjectionStore>(store: &S) -> Result<Vec<ProjectionInfo>, ProjectionStoreError> {
    let size = PageSize::new(3).map_err(|_| ProjectionStoreError::InvalidCursor)?;
    let mut request: PageRequest<ProjectionList> = PageRequest { size, after: None };
    let mut all = Vec::new();
    loop {
        let (items, next) = store.list(&request).await?.into_parts();
        all.extend(items);
        match next {
            Some(cursor) => request.after = Some(cursor),
            None => return Ok(all),
        }
    }
}

fn pending(view: &StoreView) -> usize {
    view.infos
        .iter()
        .filter(|info| {
            matches!(info, Ok(Some(info)) if matches!(
                info.status().kind(),
                ProjectionStatusKind::Queued | ProjectionStatusKind::Fitting
            ))
        })
        .count()
}

// Compared once per step and dropped; boxing would only add noise.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq)]
enum Outcome {
    Enqueued(Result<(), ProjectionStoreError>),
    Claimed(
        Result<Option<ProjectionInfo>, crosstalk_spec::interfaces::l6_analysis::ProjectionJobError>,
    ),
    Done(Result<(), crosstalk_spec::interfaces::l6_analysis::ProjectionJobError>),
    Counted(Result<u32, crosstalk_spec::interfaces::l6_analysis::ProjectionJobError>),
    Waited,
}

async fn apply<S: ProjectionStore>(store: &mut S, op: &ProjectionOp, now: u64) -> Outcome {
    let at = ts(now);
    match op {
        ProjectionOp::Enqueue { job } => match spec() {
            Some(spec) => Outcome::Enqueued(
                store
                    .enqueue(ProjectionInfo::queued(
                        projection(*job),
                        spec,
                        operator(1),
                        at,
                    ))
                    .await,
            ),
            None => Outcome::Waited,
        },
        ProjectionOp::Claim => Outcome::Claimed(store.claim(at).await),
        ProjectionOp::Complete {
            job,
            matching,
            watermark_back,
            wrong_id,
        } => {
            let id = if *wrong_id {
                projection(job + 1)
            } else {
                projection(*job)
            };
            match frame(id, *matching, now.saturating_sub(*watermark_back)) {
                Some(frame) => Outcome::Done(store.complete(projection(*job), frame, at).await),
                None => Outcome::Waited,
            }
        }
        ProjectionOp::Fail { job } => Outcome::Done(
            store
                .fail(projection(*job), FitFailure::NonFiniteLayout, at)
                .await,
        ),
        ProjectionOp::RequeueLapsed => Outcome::Counted(store.requeue_lapsed(at).await),
        ProjectionOp::Expire => Outcome::Counted(store.expire(at).await),
        ProjectionOp::Wait { .. } => Outcome::Waited,
    }
}

/// Random enqueues, claims, completions, failures, lease lapses and
/// expiries against the reference. `make` builds a fresh, empty subject
/// under the given configuration.
pub fn check_projection_store<S, F, Fut>(
    harness: HarnessConfig,
    make: F,
) -> Result<(), ModelMismatch>
where
    S: ProjectionStore,
    F: Fn(ProjectionConfig) -> Fut,
    Fut: Future<Output = S>,
{
    let strategy = prop::collection::vec(projection_op(), 1..harness.max_ops);
    run(harness, strategy, |runtime, ops| {
        runtime.block_on(async {
            let mut subject = make(projection_config()).await;
            let mut reference = InMemoryProjectionStore::new(projection_config());
            let mut now = 100u64;
            for (step, op) in ops.iter().enumerate() {
                now += match op {
                    ProjectionOp::Wait { micros } => *micros,
                    _ => 1,
                };
                let theirs = apply(&mut subject, op, now).await;
                let ours = apply(&mut reference, op, now).await;
                same(step, &format!("{op:?}"), &theirs, &ours)?;
                let observed = view(&subject).await;
                same(
                    step,
                    "state after the operation",
                    &observed,
                    &view(&reference).await,
                )?;
                holds(step, pending(&observed) <= 16, || {
                    "more than 16 jobs pending".to_owned()
                })?;
            }
            Ok::<(), Divergence>(())
        })
    })
}
