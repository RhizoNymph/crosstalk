//! Deterministic simulation tests of the surface: each runs under
//! `crosstalk-sim` (paused time on one thread, seeded) over the in-memory
//! reference stores of [`crate::tests::world`].
//!
//! - [`live`]: the live feed's cursors, heartbeats, resume, lag and ends.
//! - [`actions`]: racing alert actions, policy reads after writes, publish
//!   counts under bus faults, alert revisions and their announcements.
//! - [`reads`]: list traversals under concurrent writes and watermarks read
//!   before data.

mod actions;
mod live;
mod reads;

use crosstalk_sim::{CheckFailed, SimCtx};

/// `ctx.check`, as a function the tests can pass around.
fn check(ctx: &SimCtx, holds: bool, message: impl FnOnce() -> String) -> Result<(), CheckFailed> {
    ctx.check(holds, message)
}

/// A store or surface error as a failed check.
fn failed(what: &str, error: impl std::fmt::Debug) -> CheckFailed {
    CheckFailed::new(format!("{what}: {error:?}"))
}
