//! A projected point is always between two agents. Stand-in for the port's
//! `ProjectedPoint::new`, which refuses a point whose sender is its reader
//! (`PointWithinOneAgent`, `topology.filter.cross-agent-only`).
//!
//! Today's `ProjectedPoint` is a plain struct whose `to` may equal `from`
//! for agents merged by the time the sample was read; [`checked_point`] is
//! the check every point the UI builds goes through, so a frame never holds
//! a within-one-agent point.

use crosstalk_spec::aggregates::projection::ProjectedPoint;
use crosstalk_spec::ids::AgentId;

/// A point whose sender is its reader: a transmission within one agent,
/// which no projection holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointWithinOneAgent(pub AgentId);

/// `point`, or `PointWithinOneAgent` when its sender is its reader.
pub fn checked_point(point: ProjectedPoint) -> Result<ProjectedPoint, PointWithinOneAgent> {
    if point.from == point.to {
        return Err(PointWithinOneAgent(point.to));
    }
    Ok(point)
}
