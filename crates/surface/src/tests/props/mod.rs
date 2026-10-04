//! Property tests: generated worlds and request sequences, each case on a
//! fresh current-thread runtime, checked against a stated oracle.
//!
//! - [`actions`]: stamping, published events and the audit record of every
//!   call.
//! - [`alerts`]: the alert lifecycle model and the alerts filter.
//! - [`reads`]: list filters, the audit log, detection quality, the topic
//!   history, and what a View caller can read.
//! - [`topology`]: topology and series as the edge store answers them, and
//!   the version every topic in a response belongs to.
//! - [`channels`]: channel rows, names, the promotion preview and windows.

mod actions;
mod alerts;
mod channels;
mod reads;
mod topology;

use std::fmt::Debug;

use proptest::strategy::Strategy;
use proptest::test_runner::{Config, TestCaseError, TestError, TestRunner};

/// Run `case` on `cases` values of `strategy`, each on a fresh runtime;
/// panic with proptest's minimal failing value.
pub fn property<S, F, Fut>(cases: u32, strategy: S, case: F)
where
    S: Strategy,
    S::Value: Debug,
    F: Fn(S::Value) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&strategy, |value| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| TestCaseError::fail(format!("no runtime: {error}")))?;
        runtime.block_on(case(value)).map_err(TestCaseError::fail)
    });
    match result {
        Ok(()) => {}
        Err(TestError::Fail(reason, minimal)) => panic!("{reason}\nminimal: {minimal:#?}"),
        Err(TestError::Abort(reason)) => panic!("aborted: {reason}"),
    }
}

/// `Err(what())` unless `holds`.
pub fn ensure(holds: bool, what: impl FnOnce() -> String) -> Result<(), String> {
    if holds { Ok(()) } else { Err(what()) }
}

/// `left == right`, or a message naming both.
pub fn equal<T: PartialEq + Debug>(label: &str, left: &T, right: &T) -> Result<(), String> {
    ensure(left == right, || {
        format!("{label}:\n  got:      {left:?}\n  expected: {right:?}")
    })
}
