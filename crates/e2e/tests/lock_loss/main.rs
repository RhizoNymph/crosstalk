//! The pipeline lock lost while the gateway runs: the process never
//! stops, the proxy never refuses a connection, and the pipeline resumes
//! in process once the database answers and the lock is free again (see
//! `docs/features/postgres_mode.md`, "Losing the pipeline lock").
//!
//! - `partition`: the lock's and the pipeline's sessions silently
//!   partitioned, or reset as a Postgres stop does, past the loss's
//!   reporting and abandon bounds (and past the server's reap), then
//!   healed: zero refused connects, zero non-OK answers, `/readyz` 200
//!   throughout, the spool drained once, recovered holds ready at once and
//!   the uninterrupted run's answers. Also the graceful shutdown's bound
//!   across a partition.
//! - `rival`: the lock's session terminated while the database stays
//!   reachable, another session taking the lock for 10 s: no write by the
//!   abandoned incarnation after its abandon, the loop waits while the lock
//!   is held elsewhere, then resumes.
//! - `repeated`: five losses and resumes in one process, each returning
//!   the runtime's tasks and the server's sessions to their baseline.
//!
//! The tests take turns (`harness::serial`): each runs a gateway against
//! its own database, and the loss detection's timing must not depend on
//! another test's load. Evidence paths: `crosstalk_e2e::lock_loss::*`.

#[path = "../common/mod.rs"]
mod common;

mod harness;
mod partition;
mod repeated;
mod rival;
