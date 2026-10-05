//! The L8 conformance suite: the behaviour every implementation of
//! crosstalk's L8 traits (`QueryApi`, `OperatorActions`, `LiveFeed` and the
//! export stream) must show, as tests generic over the implementation.
//!
//! - [`harness`]: the [`Harness`] trait an implementation provides: it
//!   provisions a [`Scenario`] into a fresh backend and binds the
//!   scenario's roles to ids, and names the operator each test caller acts
//!   as and the export hasher.
//! - [`scenario`]: worlds as facts over typed roles ("agent A writes
//!   resource R, agent B reads it and the text matches"), and the named
//!   scenarios the tests run against.
//! - [`tests`]: the tests, one `async fn` per test, generic over the
//!   harness, by area. Each names the `spec/invariants` id it checks.
//! - [`suite!`]: instantiates every test for one harness.
//!
//! Assertions are relations the spec defines (an edge's transmissions page
//! holds exactly the edge's count, a merge then an unmerge leaves the
//! topology as it was, every action call leaves exactly one audit entry)
//! and what the scenario's facts imply, never totals of one generated
//! world, so the fixture and the gateway are held to the same semantics.
//!
//! See `docs/features/conformance.md` for running it against a new
//! implementation.

pub mod harness;
pub mod scenario;
mod suite;
pub mod support;
pub mod tests;

pub use harness::{Harness, Knobs, Provision, ProvisionError, Provisioned};
pub use scenario::{Bindings, Scenario};
pub use suite::{RunError, run};
