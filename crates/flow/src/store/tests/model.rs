//! Model-based tests: the Postgres stores against crosstalk-memory's
//! reference L5 stores, on the reference harnesses' generated operation
//! sequences (`crosstalk_memory::flow::registry::model` and
//! `crosstalk_memory::flow::verdicts::model`). After every step both must
//! return the same results, publish the same events (at least the
//! reference's change notifications) and read back the same: every stored
//! channel, `channel`, `canonical` and `policy_history` of every channel
//! id, a `lookup` of every locator, full `resource_use` and
//! `transmissions` traversals; every verdict log, transmission and the
//! quality tally. The registry harness also checks that declared patterns
//! never overlap, that a channel's policy is its history's current one and
//! that supersession resolves in one step to a declared channel.
//!
//! The reference harness builds its subject synchronously on a
//! current-thread runtime; a Postgres store needs a migrated database and a
//! multi-threaded runtime, so these tests drive the same proptest
//! strategies themselves: one test database, emptied before each case, and
//! each case run by the harness's own `run_case`.

use std::fmt::Debug;
use std::sync::Arc;

use crosstalk_memory::flow::registry::model::{self as registry_model, registry_ops};
use crosstalk_memory::flow::verdicts::model::{self as verdict_model, verdict_ops};
use crosstalk_memory::model::Divergence;
use crosstalk_memory::support::IdSequence;
use proptest::strategy::Strategy;
use proptest::test_runner::{Config, TestCaseError, TestError, TestRunner};
use sqlx::PgPool;
use tokio::runtime::Handle;
use tokio::sync::mpsc::unbounded_channel;

use super::support::{Failure, SequenceIds, TestResult, db, reset};
use crate::store::outbox::ChannelSink;
use crate::store::{PgChannelRegistry, PgTransmissionStore};

/// Cases and steps per model test. Every step reads the whole store back
/// over the network, so fewer cases than the in-memory harnesses run.
const REGISTRY: Budget = Budget {
    cases: 8,
    max_ops: 24,
};
const VERDICTS: Budget = Budget {
    cases: 16,
    max_ops: 30,
};

#[derive(Debug, Clone, Copy)]
struct Budget {
    cases: u32,
    max_ops: usize,
}

/// Run `case` on `budget.cases` generated inputs, each on `handle` (from inside
/// `block_in_place`), failing with the minimal failing input.
fn drive<S, F, Fut>(handle: &Handle, budget: Budget, strategy: S, case: F) -> TestResult
where
    S: Strategy,
    S::Value: Debug,
    F: Fn(S::Value) -> Fut,
    Fut: std::future::Future<Output = Result<(), Divergence>>,
{
    let mut runner = TestRunner::new(Config {
        cases: budget.cases,
        failure_persistence: None,
        ..Config::default()
    });
    let result = runner.run(&strategy, |input| {
        handle.block_on(case(input)).map_err(|divergence| {
            TestCaseError::fail(format!("step {}: {}", divergence.step, divergence.what))
        })
    });
    match result {
        Ok(()) => Ok(()),
        Err(TestError::Fail(reason, minimal)) => Err(Failure::Unexpected(format!(
            "{reason}\nminimal failing sequence: {minimal:#?}"
        ))),
        Err(TestError::Abort(reason)) => Err(Failure::Unexpected(format!("aborted: {reason}"))),
    }
}

fn setup(what: &str, error: impl std::fmt::Display) -> Divergence {
    Divergence::new(0, format!("{what}: {error}"))
}

async fn registry_case(
    pool: PgPool,
    ops: Vec<registry_model::RegistryOp>,
) -> Result<(), Divergence> {
    reset(&pool).await.map_err(|error| setup("reset", error))?;
    let agents = registry_model::directory().await?;
    let (sender, events) = unbounded_channel();
    let sut = PgChannelRegistry::open(
        pool,
        agents.clone(),
        Arc::new(SequenceIds(IdSequence::default())),
        ChannelSink::new(sender),
    )
    .await
    .map_err(|error| setup("open", error))?;
    registry_model::run_case(sut, events, agents, &ops).await
}

async fn verdict_case(pool: PgPool, ops: Vec<verdict_model::VerdictOp>) -> Result<(), Divergence> {
    reset(&pool).await.map_err(|error| setup("reset", error))?;
    let agents = registry_model::directory().await?;
    let (sender, events) = unbounded_channel();
    let sut = PgTransmissionStore::new(pool, agents.clone(), ChannelSink::new(sender));
    verdict_model::run_case(sut, events, agents, &ops).await
}

/// `PgChannelRegistry` agrees with `MemoryChannels` on every generated
/// sequence of declarations, resources, accesses, discoveries, recorded
/// transmissions, policy decisions, promotions, coverage previews, lookups,
/// detection changes and reads (INV-1032 `flow.traffic.resources-placed-by-lookup`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_agrees_with_reference_model() -> TestResult {
    let Some(db) = db("registry_agrees_with_reference_model").await? else {
        return Ok(());
    };
    let pool = db.pool().clone();
    let handle = Handle::current();
    let outcome = tokio::task::block_in_place(|| {
        drive(&handle, REGISTRY, registry_ops(REGISTRY.max_ops), |ops| {
            registry_case(pool.clone(), ops)
        })
    });
    db.close().await?;
    outcome
}

/// `PgTransmissionStore` agrees with `MemoryVerdicts` on every generated
/// sequence of saves, verdicts, logs and quality tallies, and `set` never
/// changes the stored transmission.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transmission_store_agrees_with_reference_model() -> TestResult {
    let Some(db) = db("transmission_store_agrees_with_reference_model").await? else {
        return Ok(());
    };
    let pool = db.pool().clone();
    let handle = Handle::current();
    let outcome = tokio::task::block_in_place(|| {
        drive(&handle, VERDICTS, verdict_ops(VERDICTS.max_ops), |ops| {
            verdict_case(pool.clone(), ops)
        })
    });
    db.close().await?;
    outcome
}
