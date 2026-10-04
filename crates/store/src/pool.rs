//! The connection pool.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tracing::info;

use crate::config::{DatabaseUrl, PoolSettings, StoreConfig};
use crate::error::{StoreError, classify};
use crate::extensions::{InstalledExtension, ensure_extensions};
use crate::layer::Layer;
use crate::migrate::{Migrations, migrate};

/// A connected Postgres pool. Cloning is cheap and shares the pool.
#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Opens the pool and checks that one connection succeeds, so a wrong
    /// URL or an unreachable server fails here rather than on first use.
    pub async fn connect(config: &StoreConfig) -> Result<Self, StoreError> {
        let pool = open_pool(config.url(), config.pool()).await?;
        info!(
            target_db = %config.url().target(),
            max_connections = config.pool().max_connections().get(),
            min_connections = config.pool().min_connections(),
            "postgres pool connected"
        );
        Ok(Self { pool })
    }

    /// Wraps an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The pool, for queries. Layer queries qualify tables with the layer's
    /// schema (`ingress.exchanges`); the pool's `search_path` is the
    /// server default.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Runs `layer`'s migrations; see [`migrate`].
    pub async fn migrate(
        &self,
        layer: Layer,
        migrations: Migrations<'_>,
    ) -> Result<(), StoreError> {
        migrate(&self.pool, layer, migrations).await
    }

    /// Creates the required extensions; see [`ensure_extensions`].
    pub async fn ensure_extensions(&self) -> Result<Vec<InstalledExtension>, StoreError> {
        ensure_extensions(&self.pool).await
    }

    /// Closes every connection, waiting for checked-out ones to return.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// Opens a pool with `settings` against `url` and checks one connection.
pub(crate) async fn open_pool(
    url: &DatabaseUrl,
    settings: &PoolSettings,
) -> Result<PgPool, StoreError> {
    PgPoolOptions::new()
        .max_connections(settings.max_connections().get())
        .min_connections(settings.min_connections())
        .acquire_timeout(settings.acquire_timeout())
        .connect_with(url.connect_options().clone())
        .await
        .map_err(|source| StoreError::Connect {
            target: url.target(),
            failure: classify(&source),
            source,
        })
}
