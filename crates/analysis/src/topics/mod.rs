//! The topic catalog on Postgres: [`PgTopicCatalog`], the spec's
//! `TopicCatalog` and `TopicLifecycle` over schema `analysis`
//! (`migrations/0004_topics.sql`), and the one publisher of
//! `TopicVersionDropped`.
//!
//! ```text
//! begin_fit ─▶ Fitting ─complete_fit (topics, lineage)─▶ ─mark_ready─▶ Ready ─mark_active─▶ Active
//!                 └─fail_fit: the version is removed, its number never reused
//! mark_active / unpin / enforce_retention ─▶ Dropped (sizes frozen, assignments deleted)
//! ```
//!
//! - **Writes** run in `SERIALIZABLE` transactions with bounded retries
//!   (`crosstalk_store::retry_serializable`). Each loads the version
//!   history (`rows::versions`), applies the spec's own transitions to it
//!   (`TopicVersionInfo::new`/`with_retention`, `TopicVersionHistory::new`,
//!   `pin`, `unpin`, `mark_dropped`, `RetentionPolicy::to_drop`) and stores
//!   the versions it changed, so a stored history always passes
//!   `TopicVersionHistory::new` and the store decides as the reference
//!   (`crosstalk_memory::analysis::catalog`) does. A refusal aborts the
//!   transaction: it changes nothing and publishes nothing.
//! - **Events.** `mark_ready`, `mark_active`, `pin`, `unpin` and every
//!   drop append `Changed::TopicVersion` (and `TopicVersionDropped` for a
//!   drop) to `analysis.outbox` in the same transaction; the relay
//!   publishes them after the commit ([`crate::pg::outbox`]).
//! - **Assignments** are keyed by (version, transmission): the same
//!   assignment again is `Unchanged`, so a redelivered classification is a
//!   no-op; another is `Conflicting`.
//! - **Sizes** are counted at the read from the stored assignments,
//!   resolving sender and reader through the `AgentDirectory`
//!   (`analysis.sizes.match-cross-agent-assignments`). A drop freezes the
//!   all-time sizes on the version's row and deletes its assignments in one
//!   transaction (`analysis.retention.mark-before-delete`).
//! - **Version numbers** come from a counter row, not a sequence, so a
//!   refused or retried `begin_fit` consumes no number and a failed fit's
//!   number is never given again.

mod catalog;
mod lifecycle;
mod lineage;
mod rows;

#[cfg(test)]
pub(crate) mod tests;

use std::sync::Arc;

use crosstalk_spec::aggregates::retention::RetentionPolicy;
use crosstalk_spec::aggregates::topic::TopicModelVersion;
use crosstalk_spec::aggregates::topic_history::{FitRecord, TopicVersionInfo, TopicVersionStatus};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_spec::interfaces::l6_analysis::CatalogError;
use crosstalk_spec::interfaces::l6_analysis::lifecycle::TopicLifecycleError;
use crosstalk_spec::support::{Similarity, Timestamp};
use crosstalk_store::{SerializableRetry, TxError, retry_serializable};
use sqlx::PgPool;

use crate::pg::outbox::{self, Pending};
use crate::pg::tx::{abort, failure};
use crate::pg::{CursorKey, EventSink, StorageFailure};

pub use lineage::lineage_between;

failure!(CatalogError, TopicLifecycleError);

/// The catalog's configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CatalogConfig {
    pub retention: RetentionPolicy,
    /// Every non-best lineage link is at or above it.
    pub lineage_floor: Similarity,
}

/// What the catalog is built from besides the pool and config.
pub struct CatalogParts<D, S> {
    /// Resolves an assignment's sender and reader when sizes are counted.
    pub agents: D,
    /// Where committed changes are published.
    pub sink: Arc<S>,
    pub cursor_key: CursorKey,
    pub retry: SerializableRetry,
}

/// The Postgres topic catalog. Clones share the pool and the sink.
pub struct PgTopicCatalog<D, S> {
    pool: PgPool,
    config: CatalogConfig,
    agents: Arc<D>,
    sink: Arc<S>,
    cursor_key: CursorKey,
    retry: SerializableRetry,
}

impl<D, S> Clone for PgTopicCatalog<D, S> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config,
            agents: Arc::clone(&self.agents),
            sink: Arc::clone(&self.sink),
            cursor_key: self.cursor_key,
            retry: self.retry,
        }
    }
}

impl<D, S> std::fmt::Debug for PgTopicCatalog<D, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgTopicCatalog")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<D, S> PgTopicCatalog<D, S>
where
    D: AgentDirectory + Send + Sync,
    S: EventSink,
{
    /// The catalog over `pool` (L6's migrations applied). The first open
    /// records version 0, active since `started_at`; a later open keeps
    /// what is stored (and ignores `started_at`). Events an earlier run
    /// left in the outbox are published first.
    pub async fn open(
        pool: PgPool,
        config: CatalogConfig,
        started_at: Timestamp,
        parts: CatalogParts<D, S>,
    ) -> Result<Self, StorageFailure> {
        let catalog = Self {
            pool,
            config,
            agents: Arc::new(parts.agents),
            sink: parts.sink,
            cursor_key: parts.cursor_key,
            retry: parts.retry,
        };
        catalog.flush_outbox().await?;
        let zero = TopicVersionInfo::new(
            TopicModelVersion(0),
            TopicVersionStatus::Active {
                fit: FitRecord::Unfitted,
                activated_at: started_at,
            },
        )
        .map_err(|error| StorageFailure::Invariant(format!("version 0: {error:?}")))?;
        retry_serializable(&catalog.pool, &catalog.retry, |conn| {
            Box::pin(async move {
                let created = sqlx::query(
                    "INSERT INTO analysis.topic_catalog (next_version) VALUES (1) \
                     ON CONFLICT (singleton) DO NOTHING",
                )
                .execute(&mut *conn)
                .await?;
                if created.rows_affected() == 1 {
                    rows::insert_version(conn, &zero)
                        .await
                        .map_err(|failure| failure.into_tx(|failure| failure))?;
                }
                Ok::<(), TxError<StorageFailure>>(())
            })
        })
        .await
        .map_err(StorageFailure::from)?;
        Ok(catalog)
    }

    /// Publish what an earlier run left in the outbox. Returns how many
    /// events it published.
    pub async fn flush_outbox(&self) -> Result<usize, StorageFailure> {
        Ok(outbox::flush(&self.pool, self.sink.as_ref()).await?)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn config(&self) -> CatalogConfig {
        self.config
    }

    async fn deliver(&self, pending: Pending) {
        outbox::deliver(&self.pool, self.sink.as_ref(), pending).await;
    }
}

/// A storage failure inside a lifecycle transaction.
fn lifecycle_abort(failure: impl Into<StorageFailure>) -> TxError<TopicLifecycleError> {
    abort(failure)
}

/// A storage failure inside a catalog transaction.
fn catalog_abort(failure: impl Into<StorageFailure>) -> TxError<CatalogError> {
    abort(failure)
}
