//! Integration tests against a real Postgres.
//!
//! Every test that needs a server is gated on `TEST_DATABASE_URL`: when it
//! is unset the test prints `skipping <name>: …` and passes. Start a
//! disposable server with `scripts/test-db.sh` and export the URL it prints
//! to run them. Each test gets its own database ([`TestDb`]), so they run in
//! parallel; the runtime must be multi-threaded.

mod connect;
mod extensions;
mod harness;
mod migrations;
mod retry;

use crosstalk_store::{StoreError, TestDb, TestDbError};

/// Why an integration test failed.
#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    TestDb(#[from] TestDbError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
    #[error("{0}")]
    Unexpected(String),
}

type TestResult = Result<(), Failure>;

/// A fresh database, or `None` (and a printed reason) when
/// `TEST_DATABASE_URL` is unset.
async fn db(test: &str) -> Result<Option<TestDb>, Failure> {
    Ok(TestDb::new_or_skip(test).await?)
}

/// The fixture migration directory `name` under `tests/fixtures`.
fn fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// Whether `schema.table` exists in the database.
async fn table_exists(pool: &sqlx::PgPool, schema: &str, table: &str) -> Result<bool, Failure> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
         WHERE table_schema = $1 AND table_name = $2)",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await?)
}

/// Whether a database named `name` exists on the server.
async fn database_exists(pool: &sqlx::PgPool, name: &str) -> Result<bool, Failure> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(pool)
            .await?,
    )
}
