//! Projection jobs: the store's reads and `fit_projection`.
//!
//! `fit_projection` resolves the filter's version as every linked view does
//! (errors as for one) and records a queued [`ProjectionInfo`] whose spec
//! pins it. The fixture's fitter then runs the job at once, through the
//! spec's transitions: started, then completed with its frame
//! ([`sample::fit`]) or failed with a [`FitFailure`]. Each call records a
//! new job. The generated world also holds jobs in the other states
//! ([`seed`]).
//!
//! Reads fail as `ProjectionStore` does: unknown `NotFound`, queued or
//! fitting `Conflict(ProjectionNotReady)`, failed `Conflict(ProjectionFailed)`,
//! expired `ProjectionNotRetained`.

pub mod sample;
pub mod seed;

use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::aggregates::projection::{
    InvalidTransition, Projection, ProjectionInfo, ProjectionParams, ProjectionSpec,
    ProjectionStatus, ProjectionStatusKind,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{OperatorId, ProjectionId};
use crosstalk_spec::interfaces::l6_analysis::ProjectionStoreError;
use crosstalk_spec::interfaces::l8_surface::QueryError;
use crosstalk_spec::paging::{Page, PageRequest, ProjectionList};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::backend::Result;
use crate::backend::fixture::clock::{DAY, NOW};
use crate::backend::fixture::store::{Job, State};
use crate::backend::fixture::world::World;

use super::Ctx;
use super::graph::store_error;
use super::linked::resolve_version;
use super::page::{Key, digest, paginate};
use sample::Outcome;

/// How long a frame is kept after its fit: the fixture's
/// `projection.frame_retention_days` (the gateway's default is 180).
pub const FRAME_RETENTION: u64 = 3 * DAY;

/// At most this many jobs are queued or fitting
/// (`ProjectionStore::MAX_PENDING`).
pub const MAX_PENDING: usize = 16;

fn transition(error: InvalidTransition) -> QueryError {
    store_error("projection transition", error)
}

/// Runs a queued job as the fixture's fitter: started at `at`, then ready
/// with its frame or failed.
pub fn run(ctx: &Ctx, queued: ProjectionInfo, at: Timestamp) -> Result<Job> {
    let started = queued.start(at).map_err(transition)?;
    match sample::fit(ctx, started.spec(), started.id(), at)? {
        Outcome::Fitted(fitted, frame) => {
            let ready = started.complete(fitted).map_err(transition)?;
            Projection::new(ready, *frame)
                .map(Job::ready)
                .map_err(|e| store_error("projection", e))
        }
        Outcome::Failed(failure) => started
            .fail(at, failure)
            .map(Job::record)
            .map_err(transition),
    }
}

/// What a job for `filter` over `window` records, pinned to `version`
/// under the fixture's embedding model.
pub fn spec(
    world: &World,
    window: TimeWindow,
    filter: &TopologyFilter,
    version: TopicModelVersion,
    params: ProjectionParams,
) -> ProjectionSpec {
    ProjectionSpec::new(
        window,
        filter.clone(),
        version,
        params,
        world.topics.model.clone(),
    )
}

fn pending(state: &State) -> usize {
    state
        .projections
        .iter()
        .filter(|job| {
            matches!(
                job.info().status().kind(),
                ProjectionStatusKind::Queued | ProjectionStatusKind::Fitting
            )
        })
        .count()
}

/// `fit_projection`: validate, record and run a job; returns its id.
pub fn fit(
    world: &World,
    state: &mut State,
    by: OperatorId,
    window: TimeWindow,
    filter: &TopologyFilter,
    params: ProjectionParams,
) -> Result<ProjectionId> {
    let version = resolve_version(world, filter)?;
    if pending(state) >= MAX_PENDING {
        return Err(ProjectionStoreError::QueueFull.into());
    }
    let id = ProjectionId::from_ulid(state.mint.ulid(NOW));
    let job = ProjectionInfo::queued(id, spec(world, window, filter, version, params), by, NOW);
    let job = run(&Ctx::new(world, state), job, NOW)?;
    state.projections.push(job);
    Ok(id)
}

fn job(state: &State, id: ProjectionId) -> Result<&Job> {
    state
        .projections
        .iter()
        .find(|job| job.info().id() == id)
        .ok_or_else(|| ProjectionStoreError::Unknown(id).into())
}

pub fn status(state: &State, id: ProjectionId) -> Result<ProjectionInfo> {
    job(state, id).map(|job| job.info().clone())
}

/// Every job, newest id first.
pub fn list(
    state: &State,
    page: &PageRequest<ProjectionList>,
) -> Result<Page<ProjectionInfo, ProjectionList>> {
    let items: Vec<(Key, ProjectionInfo)> = state
        .projections
        .iter()
        .map(|job| {
            let info = job.info();
            ((0, u128::MAX - info.id().as_ulid()), info.clone())
        })
        .collect();
    paginate("projections", digest(&()), items, page)
}

pub fn read(state: &State, id: ProjectionId) -> Result<Projection> {
    let error = match job(state, id)? {
        Job::Ready(projection) => return Ok(projection.as_ref().clone()),
        Job::Record(info) => match info.status() {
            ProjectionStatus::Queued | ProjectionStatus::Fitting { .. } => {
                ProjectionStoreError::NotReady {
                    projection: id,
                    status: info.status().kind(),
                }
            }
            ProjectionStatus::Failed { failure, .. } => ProjectionStoreError::Failed {
                projection: id,
                failure: failure.clone(),
            },
            ProjectionStatus::Expired { .. } => ProjectionStoreError::NotRetained(id),
            ProjectionStatus::Ready(_) => ProjectionStoreError::Store {
                reason: "a ready job stored without its frame".to_owned(),
            },
        },
    };
    Err(error.into())
}
