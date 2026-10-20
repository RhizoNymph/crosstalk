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
//! - `standby`: two gateways on one database, the owner's lock session
//!   terminated (or the owner killed) and the standby taking over: its
//!   take fences the owner's epoch, so no write of the older epoch commits
//!   once the standby's incarnation began; the old owner stands by,
//!   capturing throughout.
//! - `gate`: a gateway that does not hold the lock publishes its captures
//!   directly once every pipeline group exists (the lock holder's exchange
//!   log writes them), and on a fresh database spools until they do.
//! - `handover`: two gateways on one database, the owner shut down
//!   gracefully with traffic flowing: it hands the pipeline over (in-flight
//!   work finished or released, its pool closed, then the lock unlocked),
//!   and the standby takes it within a retry, with nothing redelivered on a
//!   later attempt, dead-lettered or logged twice.
//!
//! The tests take turns (`harness::serial`): each runs a gateway against
//! its own database, and the loss detection's timing must not depend on
//! another test's load. Evidence paths: `crosstalk_e2e::lock_loss::*`.

#[path = "../common/mod.rs"]
mod common;

mod gate;
mod handover;
mod harness;
mod partition;
mod repeated;
mod rival;
mod standby;
