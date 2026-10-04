//! The projection jobs the generated world starts with, one per status the
//! fitter does not leave a new job in:
//!
//! - **expired**: the first day under v1, fitted the day after and expired
//!   [`FRAME_RETENTION`] later; its spec is still readable;
//! - **failed**: a job pinned to v0, queued just before v2's activation
//!   dropped v0, failed without starting (`VersionNotRetained`);
//! - **fitting**: the last day, started a minute ago;
//! - **queued**: the whole week, requested thirty seconds ago behind it.

use crosstalk_spec::aggregates::edge::TopologyFilter;
use crosstalk_spec::aggregates::projection::{
    FitFailure, ProjectionInfo, ProjectionLimit, ProjectionParams,
};
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::ids::{OperatorId, ProjectionId};
use crosstalk_spec::support::{TimeWindow, Timestamp};

use crate::Result;
use crate::clock::{DAY, HOUR, MINUTE, NOW, SECOND, START, minus, plus};
use crate::store::{Job, State};
use crate::world::World;
use crate::world::topics::V2_AT;

use super::super::Ctx;
use super::super::graph::store_error;
use super::{FRAME_RETENTION, run, spec};

/// The fit form's defaults with `seed` and `limit`.
fn params(seed: u64, limit: u32) -> Result<ProjectionParams> {
    let limit = ProjectionLimit::new(limit).map_err(|e| store_error("seed limit", e))?;
    ProjectionParams::new(
        limit,
        ProjectionParams::DEFAULT_NEIGHBORS,
        ProjectionParams::DEFAULT_MIN_DIST_MILLI,
        seed,
    )
    .map_err(|e| store_error("seed params", e))
}

fn window(start: Timestamp, end: Timestamp) -> Result<TimeWindow> {
    TimeWindow::new(start, end).map_err(|e| store_error("seed window", e))
}

/// A queued job requested at `at`.
fn queued(
    world: &World,
    state: &mut State,
    request: (TimeWindow, TopicModelVersion, ProjectionParams),
    by: OperatorId,
    at: Timestamp,
) -> ProjectionInfo {
    let (window, version, params) = request;
    let id = ProjectionId::from_ulid(state.mint.ulid(at));
    let filter = TopologyFilter::default();
    ProjectionInfo::queued(id, spec(world, window, &filter, version, params), by, at)
}

/// Adds the seeded jobs to `state`, oldest first.
pub fn seed(world: &World, state: &mut State, by: OperatorId) -> Result<()> {
    let first_day = window(START, plus(START, DAY))?;
    let transition = |e| store_error("seed transition", e);

    let requested = plus(START, DAY + HOUR);
    let expired = queued(
        world,
        state,
        (first_day, TopicModelVersion(1), params(42, 5_000)?),
        by,
        requested,
    );
    let fitted_at = plus(requested, MINUTE);
    let expired = match run(&Ctx::new(world, state), expired, fitted_at)? {
        Job::Ready(projection) => Job::record(
            projection
                .info()
                .clone()
                .expire(plus(fitted_at, FRAME_RETENTION))
                .map_err(transition)?,
        ),
        failed @ Job::Record(_) => failed,
    };
    state.projections.push(expired);

    let dropped = TopicModelVersion(0);
    let failed = queued(
        world,
        state,
        (first_day, dropped, params(42, 5_000)?),
        by,
        minus(V2_AT, 5 * MINUTE),
    )
    .fail(V2_AT, FitFailure::VersionNotRetained { version: dropped })
    .map_err(transition)?;
    state.projections.push(Job::record(failed));

    let last_day = window(minus(NOW, DAY), NOW)?;
    let active = state.active_version();
    let fitting = queued(
        world,
        state,
        (last_day, active, params(7, 20_000)?),
        by,
        minus(NOW, 2 * MINUTE),
    )
    .start(minus(NOW, MINUTE))
    .map_err(transition)?;
    state.projections.push(Job::record(fitting));

    let week = window(START, NOW)?;
    let waiting = queued(
        world,
        state,
        (week, active, params(8, 50_000)?),
        by,
        minus(NOW, 30 * SECOND),
    );
    state.projections.push(Job::record(waiting));
    Ok(())
}
