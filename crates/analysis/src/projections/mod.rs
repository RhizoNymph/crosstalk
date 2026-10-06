//! Projection jobs and frames on Postgres: [`PgProjectionStore`], the
//! spec's `ProjectionStore` over schema `analysis`
//! (`migrations/0005_projections.sql`).
//!
//! - **Jobs** move only through the spec's own transitions
//!   (`ProjectionInfo::start`, `requeue`, `complete`, `fail`, `expire`)
//!   applied to the stored record, so every stored job keeps its
//!   invariants and the store decides as the reference
//!   (`crosstalk_memory::analysis::projection`) does. Each write is one
//!   `SERIALIZABLE` transaction; `enqueue` counts the pending jobs in it, so
//!   concurrent enqueues never pass `ProjectionStore::MAX_PENDING`
//!   (`analysis.projection.queue-bounded`).
//! - **Claims** take the oldest queued job (`requested_at`, then id) under
//!   `FOR UPDATE SKIP LOCKED`, so concurrent fitters never wait on each
//!   other's claim, and record a lease. A fitting job whose lease lapsed
//!   returns to `Queued` on `requeue_lapsed` and is claimed again before
//!   younger jobs: a fitter crash never fails a job and never leaves it
//!   fitting.
//! - **Frames** are stored as `bytea` in `ProjectionFrame::encode`'s layout
//!   with the job's `Ready` record in one transaction, and decoded on read.
//!   `expire` drops the frame of every job fitted more than the frame
//!   retention before `now`.
//! - **Time** is always an argument: leases and expiry compare the times
//!   passed in, never `now()`.
//! - **Events**: `complete`, `fail` and each expiry append
//!   `Changed::Projection` to the outbox in the same transaction.

mod store;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::interfaces::l6_analysis::{ProjectionJobError, ProjectionStoreError};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::SerializableRetry;
use sqlx::PgPool;

use crate::pg::outbox::{self, Pending};
use crate::pg::tx::failure;
use crate::pg::{CursorKey, EventSink, StorageFailure};

failure!(ProjectionJobError, ProjectionStoreError);

/// How long a claim holds a job, and how long a ready frame is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionStoreConfig {
    pub lease: Duration,
    /// `projection.frame_retention_days`.
    pub frame_retention: Duration,
}

/// `at + by`, saturating at the largest timestamp.
pub fn plus(at: Timestamp, by: Duration) -> Timestamp {
    let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
    Timestamp::from_micros(at.as_micros().saturating_add(micros))
}

/// What the store is built from besides the pool and config.
pub struct ProjectionParts<S> {
    /// Where committed changes are published.
    pub sink: Arc<S>,
    pub cursor_key: CursorKey,
    pub retry: SerializableRetry,
}

/// The Postgres projection store. Clones share the pool and the sink.
pub struct PgProjectionStore<S> {
    pool: PgPool,
    config: ProjectionStoreConfig,
    sink: Arc<S>,
    cursor_key: CursorKey,
    retry: SerializableRetry,
}

impl<S> Clone for PgProjectionStore<S> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config,
            sink: Arc::clone(&self.sink),
            cursor_key: self.cursor_key,
            retry: self.retry,
        }
    }
}

impl<S> std::fmt::Debug for PgProjectionStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgProjectionStore")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<S: EventSink> PgProjectionStore<S> {
    /// The store over `pool` (L6's migrations applied). Events an earlier
    /// run left in the outbox are published first.
    pub async fn open(
        pool: PgPool,
        config: ProjectionStoreConfig,
        parts: ProjectionParts<S>,
    ) -> Result<Self, StorageFailure> {
        let store = Self {
            pool,
            config,
            sink: parts.sink,
            cursor_key: parts.cursor_key,
            retry: parts.retry,
        };
        store.flush_outbox().await?;
        Ok(store)
    }

    /// Publish what an earlier run left in the outbox. Returns how many
    /// events it published.
    pub async fn flush_outbox(&self) -> Result<usize, StorageFailure> {
        Ok(outbox::flush(&self.pool, self.sink.as_ref()).await?)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn config(&self) -> ProjectionStoreConfig {
        self.config
    }

    async fn deliver(&self, pending: Pending) {
        outbox::deliver(&self.pool, self.sink.as_ref(), pending).await;
    }
}
