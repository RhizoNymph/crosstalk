//! Postgres infrastructure for crosstalk: the connection pool, per-layer schema
//! migrations, required extensions, typed errors with retry classification,
//! and the test database harness.
//!
//! - [`StoreConfig`] reads `DATABASE_URL` from the environment and takes the
//!   pool sizing ([`PoolSettings`]) from structured config;
//!   [`Store::connect`] opens the pool.
//! - Each layer crate owns `crates/<layer>/migrations/` and a schema named
//!   after the layer ([`Layer::schema`]). It embeds its migrations with
//!   `sqlx::migrate!("./migrations")` and runs them with [`migrate`], which
//!   keeps each layer's applied versions in its own schema's table.
//! - [`ensure_extensions`] creates `vector` and `pg_trgm`.
//! - [`classify`] maps a [`sqlx::Error`] to a [`DbFailure`] that layer crates
//!   map into their spec errors (a statement the server cancelled at its
//!   `statement_timeout` or `lock_timeout` is a transient
//!   [`DbFailure::TimedOut`], counted by [`statement_timeouts`]), and
//!   [`retry_serializable`] runs a
//!   `SERIALIZABLE` transaction with bounded retries.
//! - [`ColumnWindow`] maps a `TimeWindow` onto a `bigint` time column: an
//!   end past `i64::MAX` is no upper bound, a start past it no stored time.
//! - [`TestDb`] gives each test a fresh database on the `TEST_DATABASE_URL`
//!   server.
//!
//! The SQL library is sqlx with runtime-checked queries (decision D2; see
//! `docs/features/store.md`).
//!
//! Roadmap: P1.5 (`crosstalk-store`). Infrastructure: layer crates may depend
//! on it, and it depends on no layer crate.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

mod config;
mod error;
mod extensions;
mod layer;
mod migrate;
mod pool;
mod retention;
mod retry;
mod test_db;
mod timeouts;
mod window;

pub use config::{
    ConfigError, DATABASE_URL_VAR, DatabaseUrl, PoolSettings, StoreConfig, TEST_DATABASE_URL_VAR,
    UrlProblem,
};
pub use error::{DbFailure, ExtensionProblem, StoreError, classify};
pub use extensions::{Extension, InstalledExtension, ensure_extension, ensure_extensions};
pub use layer::{Layer, MIGRATIONS_TABLE};
pub use migrate::{Migrations, migrate};
pub use pool::Store;
pub use retention::{
    RETENTION_BOUND, STATEMENT_BOUND, raise_for_retention, restore_after_retention,
};
pub use retry::{
    InvalidSerializableRetry, SerializableError, SerializableRetry, TxError, TxFuture,
    retry_serializable,
};
pub use test_db::{
    RuntimeProblem, STALE_TEMPLATE_AGE, TEST_DB_PREFIX, TEST_TEMPLATE_PREFIX, TemplateDb,
    TemplateStep, TestDb, TestDbError, TestDbName, drop_stale_templates, test_server,
};
pub use timeouts::{TimeoutCounts, TimeoutKind, observe as observe_timeouts, statement_timeouts};
pub use window::{ColumnBounds, ColumnWindow};

/// The pinned sqlx, so layer crates name its types (`PgPool`, `Migrator`)
/// through the same version. A layer crate that embeds migrations with
/// `sqlx::migrate!` also depends on `sqlx` itself (workspace pin), because
/// the macro expands to `::sqlx` paths.
pub use sqlx;
