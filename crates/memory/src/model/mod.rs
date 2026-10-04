//! Model-based property harnesses: given a store implementation and the
//! reference, random operation sequences must give equal observable
//! results. One `check_<trait>` per stateful trait; the Postgres stores
//! reuse them with their own `make`.
//!
//! Each harness:
//!
//! - generates a sequence of operations with proptest (ids from small
//!   pools, so operations collide on purpose);
//! - builds a fresh subject with the caller's `make` and a fresh reference
//!   for every case;
//! - applies every operation to both, and after each compares what both
//!   return, and what both then read back, as observable values (cursor
//!   tokens are never compared: traversals are followed to the end, and
//!   store-assigned ids are mapped by creation order);
//! - checks some invariants on the subject's results against independent
//!   oracles kept by the harness itself (the reference folds the
//!   invariants name).
//!
//! A failure is a [`ModelMismatch`] carrying proptest's minimal failing
//! sequence. Run against the reference itself (`make` building a reference
//! store), a harness checks the reference's determinism and the oracles.
//!
//! The stores' trait methods are async; each case runs on its own
//! current-thread tokio runtime, so `make` may connect to a database.

pub mod analysis;
pub mod build;
pub mod surface;
pub mod topology;

use std::fmt::Debug;

use proptest::strategy::Strategy;
use proptest::test_runner::{Config, TestCaseError, TestError, TestRunner};

/// How many random sequences a harness runs, and how long they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HarnessConfig {
    pub cases: u32,
    pub max_ops: usize,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            cases: 48,
            max_ops: 40,
        }
    }
}

/// Why a harness failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelMismatch {
    /// The subject and the reference disagreed, or an oracle failed, on the
    /// minimal sequence proptest found.
    #[error("{reason}\nminimal failing sequence: {minimal}")]
    Failed { reason: String, minimal: String },
    /// The harness could not run: a runtime or a store could not be built.
    #[error("the harness could not run: {0}")]
    Setup(String),
}

/// One step's disagreement, before proptest shrinks it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Divergence {
    pub step: usize,
    pub what: String,
}

impl Divergence {
    pub fn new(step: usize, what: impl Into<String>) -> Self {
        Self {
            step,
            what: what.into(),
        }
    }
}

/// `Ok` when the subject's and the reference's observations are equal.
pub fn same<T: PartialEq + Debug>(
    step: usize,
    label: &str,
    subject: &T,
    reference: &T,
) -> Result<(), Divergence> {
    if subject == reference {
        Ok(())
    } else {
        Err(Divergence::new(
            step,
            format!("{label}:\n  subject:   {subject:?}\n  reference: {reference:?}"),
        ))
    }
}

/// `Ok` when an oracle holds.
pub fn holds(
    step: usize,
    condition: bool,
    what: impl FnOnce() -> String,
) -> Result<(), Divergence> {
    if condition {
        Ok(())
    } else {
        Err(Divergence::new(step, what()))
    }
}

/// Run `case` on `config.cases` inputs from `strategy` (operation
/// sequences, for the store harnesses), each on a fresh current-thread
/// runtime. The one runner every harness and property test in this crate
/// shares.
pub(crate) fn run<S, F>(config: HarnessConfig, strategy: S, case: F) -> Result<(), ModelMismatch>
where
    S: Strategy,
    S::Value: Debug,
    F: Fn(&tokio::runtime::Runtime, &S::Value) -> Result<(), Divergence>,
{
    let mut runner = TestRunner::new(Config {
        cases: config.cases,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&strategy, |ops| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| TestCaseError::fail(format!("no runtime: {error}")))?;
        case(&runtime, &ops).map_err(|divergence| {
            TestCaseError::fail(format!("step {}: {}", divergence.step, divergence.what))
        })
    });
    match result {
        Ok(()) => Ok(()),
        Err(TestError::Fail(reason, minimal)) => Err(ModelMismatch::Failed {
            reason: reason.to_string(),
            minimal: format!("{minimal:#?}"),
        }),
        Err(TestError::Abort(reason)) => Err(ModelMismatch::Setup(reason.to_string())),
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod mutants;
