//! Postgres tests (`crosstalk_provenance::integration::<name>`), gated on
//! `TEST_DATABASE_URL` through `crosstalk-store`'s `TestDb`: each test gets
//! a fresh database with L4's migrations, and skips with a printed reason
//! when no server is configured.

mod engine;
mod index;

use std::time::Duration;

use crosstalk_store::{Layer, Migrations, TestDb};
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::store::MIGRATIONS;

/// A migrated test database, or `None` (skipped) when none is configured.
pub async fn database(test: &str) -> Option<TestDb> {
    let db = TestDb::new_or_skip(test).await.expect("test database")?;
    db.migrate(Layer::Provenance, Migrations::Embedded(&MIGRATIONS))
        .await
        .expect("L4 migrations");
    Some(db)
}

/// A pool on `options` that connects on first use, on whichever runtime
/// uses it: no maintenance task is spawned (no idle timeout, no lifetime,
/// no minimum), so it can be built outside a runtime.
pub fn lazy_pool_with(options: PgConnectOptions) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .min_connections(0)
        .idle_timeout(None)
        .max_lifetime(None)
        .acquire_timeout(Duration::from_secs(30))
        .connect_lazy_with(options)
}

/// A lazy pool on `db`.
pub fn lazy_pool(db: &TestDb) -> PgPool {
    lazy_pool_with(db.url().connect_options().clone())
}

/// `provenance.index.cutoff-not-inserted`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_insert_skips_fingerprints_above_cutoff() {
    index::insert_skips_fingerprints_above_cutoff().await;
}

/// `provenance.index.cutoff-not-returned`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_lookup_ignores_fingerprints_above_cutoff() {
    index::lookup_ignores_fingerprints_above_cutoff().await;
}

/// `provenance.index.frequency-counts-observed-texts`: the model-based
/// harness against `crosstalk-memory`'s reference index.
#[tokio::test(flavor = "multi_thread")]
async fn pg_frequency_matches_observation_model() {
    index::matches_the_reference_model().await;
}

/// `provenance.index.no-text`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_index_schema_has_no_text_columns() {
    index::schema_has_no_text_columns().await;
}

/// `provenance.index.retention-bound`: spans past retention are evicted.
#[tokio::test(flavor = "multi_thread")]
async fn pg_index_evicts_expired_spans() {
    engine::evicts_expired_spans().await;
}

/// `provenance.index.retention-bound`: observations age out.
#[tokio::test(flavor = "multi_thread")]
async fn pg_observations_age_out() {
    index::observations_age_out().await;
}

/// `provenance.match.none-after-expiry`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_lookup_after_evict_has_no_hits() {
    index::lookup_after_evict_has_no_hits().await;
}

/// The engine over Postgres gives the events the in-memory engine gives,
/// and its records answer the reads the conversation view needs.
#[tokio::test(flavor = "multi_thread")]
async fn pg_engine_agrees_with_memory() {
    engine::agrees_with_memory().await;
}

/// A redelivered delta republishes the same envelopes and changes no
/// record or posting.
#[tokio::test(flavor = "multi_thread")]
async fn pg_redelivered_delta_is_idempotent() {
    engine::redelivery_is_idempotent().await;
}

/// Scan status per exchange and per message, and the match reads.
#[tokio::test(flavor = "multi_thread")]
async fn pg_store_reads_scan_status_and_matches() {
    engine::scan_status_and_match_reads().await;
}

/// `provenance.index.forwarded-indexed` on Postgres: a forwarded span is
/// indexed, matched and expired as in memory, its state left `Relayed`.
#[tokio::test(flavor = "multi_thread")]
async fn pg_forwarded_spans_index_and_expire() {
    engine::forwarded_spans_index_and_expire().await;
}
