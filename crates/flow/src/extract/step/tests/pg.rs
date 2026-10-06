//! `PgExtractionLedger` against `MemoryExtractionLedger`: the step over
//! either ledger hands over the same inputs for every delta of a generated
//! sequence (redeliveries included) and ends with the same ledger, read
//! back whole.

use crosstalk_store::{TestDb, TestDbError};
use proptest::test_runner::{Config, TestCaseError, TestError, TestRunner};
use sqlx::PgPool;
use tokio::runtime::Handle;

use super::script::{Step, script};
use super::support::{Harness, Recorder};
use crate::extract::ExtractConfig;
use crate::extract::step::MemoryExtractionLedger;
use crate::store::{FlowStoreError, PgExtractionLedger, migrate};

/// Cases and deltas per case: every case reads the ledger back over the
/// network.
const CASES: u32 = 12;
const MAX_DELTAS: usize = 14;

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    TestDb(#[from] TestDbError),
    #[error(transparent)]
    Store(#[from] FlowStoreError),
    #[error("{0}")]
    Diverged(String),
}

async fn reset(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "TRUNCATE flow.extract_contexts, flow.extract_pending, flow.extract_history, \
         flow.extract_delivered, flow.extract_done",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn case(pool: PgPool, steps: Vec<Step>) -> Result<(), String> {
    reset(&pool)
        .await
        .map_err(|error| format!("reset: {error}"))?;
    let reference = Harness::new(MemoryExtractionLedger::new(), ExtractConfig::default());
    let subject = Harness::new(PgExtractionLedger::new(pool), ExtractConfig::default());
    for (index, step) in steps.iter().enumerate() {
        let delta = step.delta(10 + u128::try_from(index).unwrap_or(0));
        let runs = if step.again { 2 } else { 1 };
        for run in 0..runs {
            let (mut want, mut got) = (Recorder::default(), Recorder::default());
            let want_outcome = reference
                .run(&delta, &mut want)
                .await
                .map_err(|error| format!("delta {index}: reference: {error}"))?;
            let got_outcome = subject
                .run(&delta, &mut got)
                .await
                .map_err(|error| format!("delta {index}: postgres: {error}"))?;
            if want_outcome != got_outcome || want.0 != got.0 {
                return Err(format!(
                    "delta {index} run {run}:\n  postgres: {got_outcome:?} {:?}\n  memory:   {want_outcome:?} {:?}",
                    got.0, want.0
                ));
            }
        }
    }
    let got = subject
        .step
        .ledger()
        .state()
        .await
        .map_err(|error| format!("reading the ledger: {error}"))?;
    let want = reference.step.ledger().state();
    if got != want {
        return Err(format!(
            "ledgers differ:\n  postgres: {got:?}\n  memory:   {want:?}"
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_ledger_agrees_with_the_memory_ledger() -> Result<(), Failure> {
    let Some(db) = TestDb::new_or_skip("pg_ledger_agrees_with_the_memory_ledger").await? else {
        return Ok(());
    };
    migrate(&db.store()).await?;
    let pool = db.pool().clone();
    let handle = Handle::current();
    let outcome = tokio::task::block_in_place(|| {
        let mut runner = TestRunner::new(Config {
            cases: CASES,
            failure_persistence: None,
            ..Config::default()
        });
        runner.run(&script(MAX_DELTAS), |steps| {
            handle
                .block_on(case(pool.clone(), steps))
                .map_err(TestCaseError::fail)
        })
    });
    db.close().await?;
    match outcome {
        Ok(()) => Ok(()),
        Err(TestError::Fail(reason, minimal)) => Err(Failure::Diverged(format!(
            "{reason}\nminimal failing sequence: {minimal:#?}"
        ))),
        Err(TestError::Abort(reason)) => Err(Failure::Diverged(format!("aborted: {reason}"))),
    }
}
