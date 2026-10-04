//! The runner every model-based harness shares: generate operation
//! sequences with proptest, run each case on a fresh current-thread tokio
//! runtime, and panic with the minimal failing sequence.

use std::fmt::Debug;
use std::future::Future;

use proptest::strategy::Strategy;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

/// How hard a harness tries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HarnessConfig {
    /// Generated operation sequences per run.
    pub cases: u32,
    /// The longest sequence generated.
    pub max_ops: usize,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            cases: 64,
            max_ops: 40,
        }
    }
}

/// A mismatch between the store under test and the reference, or a broken
/// invariant: what failed, at which step.
pub type Mismatch = String;

/// Fail a case unless `sut == model`.
pub fn same<T: PartialEq + Debug>(
    step: usize,
    what: &str,
    sut: &T,
    model: &T,
) -> Result<(), Mismatch> {
    if sut == model {
        Ok(())
    } else {
        Err(format!(
            "step {step}: {what} differs\n  store under test: {sut:?}\n  reference:        {model:?}"
        ))
    }
}

/// Run `case` on `cases` inputs drawn from `strategy`, each on a fresh
/// runtime. Panics with proptest's shrunk counterexample on failure.
pub fn run<S, F, Fut>(name: &str, config: HarnessConfig, strategy: S, case: F)
where
    S: Strategy,
    S::Value: Debug,
    F: Fn(S::Value) -> Fut,
    Fut: Future<Output = Result<(), Mismatch>>,
{
    let mut runner = TestRunner::new(Config {
        cases: config.cases,
        failure_persistence: None,
        ..Config::default()
    });
    let outcome = runner.run(&strategy, |input| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .map_err(|error| TestCaseError::fail(format!("runtime: {error}")))?;
        runtime.block_on(case(input)).map_err(TestCaseError::fail)
    });
    if let Err(failure) = outcome {
        panic!("{name}: the store under test disagrees with the reference: {failure}");
    }
}
