//! Test support for L6's Postgres stores: a migrated test database, fresh
//! pools for the memory crate's model harnesses (which run each case on a
//! runtime of their own), and fixed keys.

use crosstalk_spec::ids::mint::{SeededRandom, UlidGenerator};
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_store::{DatabaseUrl, SerializableRetry, TestDb};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use super::{CursorKey, run_migrations};

pub const CURSOR_KEY: CursorKey = CursorKey::new([42; 32]);

/// A clock that never moves: ids are minted at the times passed in.
#[derive(Debug)]
pub struct Epoch;

impl Clock for Epoch {
    fn now(&self) -> Timestamp {
        Timestamp::from_micros(0)
    }
}

pub fn ids(seed: u64) -> UlidGenerator<SeededRandom> {
    UlidGenerator::new(std::sync::Arc::new(Epoch), SeededRandom::new(seed))
}

/// A serializable retry generous enough for concurrent tests.
pub fn retry() -> SerializableRetry {
    SerializableRetry::new(
        std::num::NonZeroU32::new(30).unwrap_or(std::num::NonZeroU32::MIN),
        std::time::Duration::from_millis(1),
        std::time::Duration::from_millis(50),
    )
    .unwrap_or_default()
}

/// A fresh, migrated test database, or `None` (the test is skipped) when
/// `TEST_DATABASE_URL` is not configured. Panics on any other failure: a
/// configured database that cannot be prepared is a failed test.
pub async fn database(test: &str) -> Option<TestDb> {
    let db = match TestDb::new_or_skip(test).await {
        Ok(db) => db?,
        Err(error) => panic!("test database for {test}: {error}"),
    };
    if let Err(error) = db.ensure_extensions().await {
        panic!("extensions for {test}: {error}");
    }
    if let Err(error) = run_migrations(db.pool()).await {
        panic!("migrations for {test}: {error}");
    }
    Some(db)
}

/// Every table L6 owns, emptied: the state a harness case starts from.
pub async fn truncate(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "TRUNCATE analysis.search_docs, analysis.search_embeddings, analysis.search_verdicts, \
         analysis.search_model, analysis.search_dropped_models, analysis.outbox, \
         analysis.alerts, analysis.alert_rules, analysis.alert_rule_state, analysis.alert_verdicts",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// A small pool on the current runtime over `url`'s database, emptied.
pub async fn case_pool(url: &DatabaseUrl) -> PgPool {
    let pool = match PgPoolOptions::new()
        .max_connections(2)
        .connect_with(url.connect_options().clone())
        .await
    {
        Ok(pool) => pool,
        Err(error) => panic!("connecting a harness case: {error}"),
    };
    if let Err(error) = truncate(&pool).await {
        panic!("emptying the tables: {error}");
    }
    pool
}

/// Run a model harness (which builds a runtime per case) on a thread of
/// its own, from inside a multi-threaded tokio test.
pub fn off_runtime<T: Send + 'static>(harness: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::block_in_place(|| match std::thread::spawn(harness).join() {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    })
}

/// A sink that takes every event and keeps none.
#[derive(Debug, Clone, Copy, Default)]
pub struct DiscardSink;

impl super::EventSink for DiscardSink {
    async fn publish(
        &self,
        _events: Vec<crosstalk_spec::events::BusEvent>,
    ) -> Result<(), super::SinkError> {
        Ok(())
    }
}
