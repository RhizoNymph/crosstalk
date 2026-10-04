//! Deterministic simulation for crosstalk: a virtual clock, a seeded RNG,
//! fault injection for the bus, the stores and upstreams, and the driver
//! that replays a scenario from a seed.
//!
//! The kit for every invariant whose `requires` lists `dst`. A scenario is
//! an async function of a [`SimCtx`]; [`Sim::run`] runs it from one
//! [`Seed`] on a current-thread tokio runtime with paused time, and the run
//! (every interleaving, every injected fault, every clock reading) is a
//! function of that seed. A failure reports the seed and the step, and
//! `CROSSTALK_SIM_SEED=<n>` reruns it.
//!
//! - [`rng`]: [`Seed`], [`SimRng`] (SplitMix64), [`Probability`],
//!   [`DurationRange`].
//! - [`clock`]: [`SimClock`], the spec's
//!   [`Clock`](crosstalk_spec::support::Clock) on paused time, with steps
//!   and per-node skew.
//! - [`plan`]: [`FaultPlan`], the typed description of what to inject.
//! - [`bus`]: [`FaultyBus`], any spec `EventBus` with delay, reorder,
//!   duplicate, drop-and-redeliver and crash faults per subject.
//! - [`store`]: [`FaultyStore`], latency, failure before or after commit,
//!   and crash after commit around any store call.
//! - [`upstream`]: [`UpstreamFaultInjector`], the fault for each upstream
//!   exchange a fake upstream serves.
//! - [`node`]: [`Node`], crash reports and the restarting supervisor.
//! - [`trace`]: the run's event trace and its hash.
//! - [`driver`]: [`Sim`], [`SimCtx`], [`SimConfig`], seed selection,
//!   [`sim_test()`] and the [`sim_test!`] macro.
//!
//! Decision D4: tokio's paused time plus this crate's fault layer, not
//! turmoil. The bus and the stores are in-process, and turmoil's value is
//! a simulated network (see `docs/features/sim.md`).
//!
//! Roadmap: P1.3 (`crosstalk-sim`). A dev-dependency of the layer crates,
//! never a normal one.

pub mod bus;
pub mod clock;
pub mod driver;
pub mod node;
pub mod plan;
pub mod rng;
pub mod store;
pub mod trace;
pub mod upstream;

pub use bus::{FaultyBus, FaultySubscription};
pub use clock::{ClockStep, SimClock};
pub use driver::{
    CheckFailed, FailureCause, RunSummary, SEED_VAR, SEEDS_VAR, SeedEnvError, SeedSelection, Sim,
    SimConfig, SimCtx, SimFailure, SimReport, SimTask, TaskFailed, sim_test,
};
pub use node::{Incarnation, Node, NodeHandle, SuperviseError};
pub use plan::{
    BusFaults, DropFault, ErrorStatus, FaultPlan, Redelivery, Reorder, StatusFault, StoreFaults,
    SubjectFaults, Timed, TruncateFault, UpstreamFaults,
};
pub use rng::{DurationRange, Probability, Seed, SimRng};
pub use store::{FaultyStore, InjectedFault, StoreFaultKind};
pub use trace::{
    FaultEvent, FaultKind, FaultSite, NodeName, Trace, TraceEvent, TraceHash, TraceRecord, Tracer,
};
pub use upstream::{UpstreamFault, UpstreamFaultInjector};

#[cfg(test)]
mod tests;
