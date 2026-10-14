//! `PgTransmissionStore::modify` against Postgres: extensions and a
//! classification of one transmission racing on its row.

use crosstalk_memory::flow::verdicts::race::race;

use super::support::{Failure, TestResult, db, transmissions};

/// INV-1230 `flow.transmission-store.modify-atomic`: content extended from
/// several tasks while a classification holds its locked read keeps every
/// match, and the classification.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_modify_keeps_concurrent_extensions_and_classification() -> TestResult {
    let Some(db) = db("pg_modify_keeps_concurrent_extensions_and_classification").await? else {
        return Ok(());
    };
    let (store, _events) = transmissions(db.pool()).await?;
    for round in 0..4 {
        race(store.clone(), round)
            .await
            .map_err(|error| Failure::Unexpected(format!("round {round}: {error}")))?;
    }
    Ok(())
}
