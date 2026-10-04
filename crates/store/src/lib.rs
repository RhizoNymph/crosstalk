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
//!   map into their spec errors, and [`retry_serializable`] runs a
//!   `SERIALIZABLE` transaction with bounded retries.
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
mod retry;
mod test_db;

pub use config::{
    ConfigError, DATABASE_URL_VAR, DatabaseUrl, PoolSettings, StoreConfig, TEST_DATABASE_URL_VAR,
    UrlProblem,
};
pub use error::{DbFailure, ExtensionProblem, StoreError, classify};
pub use extensions::{Extension, InstalledExtension, ensure_extension, ensure_extensions};
pub use layer::{Layer, MIGRATIONS_TABLE};
pub use migrate::{Migrations, migrate};
pub use pool::Store;
pub use retry::{
    InvalidSerializableRetry, SerializableError, SerializableRetry, TxError, TxFuture,
    retry_serializable,
};
pub use test_db::{RuntimeProblem, TEST_DB_PREFIX, TestDb, TestDbError, TestDbName};

/// The pinned sqlx, so layer crates name its types (`PgPool`, `Migrator`)
/// through the same version. A layer crate that embeds migrations with
/// `sqlx::migrate!` also depends on `sqlx` itself (workspace pin), because
/// the macro expands to `::sqlx` paths.
pub use sqlx;
