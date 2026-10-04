//! L3's agent store on Postgres: [`PgAgents`].
//!
//! One store implements every L3 agent trait over schema `reconstruct`:
//! `AgentDirectory` (from the in-process copy of the merge table,
//! [`cache`]), `IdentityResolver` (the merge log with exact unmerges,
//! vetoes, renames, and the evidence lookup behind `resolve`),
//! `AgentLifecycle`, `ClaimStore`, `ActivityStore` and `AgentReads`.
//!
//! - **Writes** run in `SERIALIZABLE` transactions with bounded retries
//!   (`crosstalk_store::retry_serializable`), so concurrent merges, and a
//!   merge racing a resolver merge, leave states some serial order would
//!   (`reconstruct.agent-merge.serializable`). A merge or unmerge loads the
//!   agent table into a [`table::Table`], decides there, and writes back
//!   what the decision changed.
//! - **Events** are appended to `reconstruct.outbox` in the transaction
//!   that makes the change and handed to the [`EventSink`] after the
//!   commit; the rows are deleted once the sink took them
//!   ([`PgAgents::flush_outbox`] republishes leftovers).
//! - **Reads** run in a `REPEATABLE READ` read-only transaction, so a
//!   profile's aliases, claims and last-seen time describe one snapshot.
//! - **Time** is always an argument; merge ids come from the [`IdSource`]
//!   the store was built with.

pub(crate) mod cache;
pub mod codec;
mod load;
mod reads;
mod resolve;
pub(crate) mod table;
mod writes;


use std::sync::Arc;

use crosstalk_spec::ids::{AgentId, MergeId};
use crosstalk_spec::interfaces::l3_reconstruction::AgentDirectory;
use crosstalk_store::{Layer, Migrations, SerializableRetry, StoreError, migrate};
use sqlx::PgPool;

use crate::error::StorageFailure;
use crate::ids::IdSource;
use crate::publish::EventSink;

use cache::DirectoryCache;

/// L3's embedded migrations (`crates/reconstruct/migrations`), run in
/// schema `reconstruct`.
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run L3's migrations on `pool`.
pub async fn run_migrations(pool: &PgPool) -> Result<(), StoreError> {
    migrate(pool, Layer::Reconstruct, Migrations::Embedded(&MIGRATIONS)).await
}

/// The Postgres L3 agent store. Clones share the pool, the directory cache,
/// the sink and the id source.
pub struct PgAgents<S, M> {
    pool: PgPool,
    retry: SerializableRetry,
    directory: Arc<DirectoryCache>,
    sink: Arc<S>,
    merge_ids: Arc<M>,
    /// Keys the agents list's cursors, so a cursor this store did not issue
    /// is refused.
    cursor_key: [u8; 32],
}

impl<S, M> Clone for PgAgents<S, M> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            retry: self.retry,
            directory: Arc::clone(&self.directory),
            sink: Arc::clone(&self.sink),
            merge_ids: Arc::clone(&self.merge_ids),
            cursor_key: self.cursor_key,
        }
    }
}

impl<S, M> std::fmt::Debug for PgAgents<S, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAgents").finish_non_exhaustive()
    }
}

impl<S, M> PgAgents<S, M>
where
    S: EventSink,
    M: IdSource<MergeId> + 'static,
{
    /// The store over `pool`, whose `reconstruct` schema is migrated
    /// ([`run_migrations`]). Loads the merge table into the directory
    /// cache. `cursor_key` keys the agents list's cursors; every node
    /// serving one deployment's list should share it.
    pub async fn open(
        pool: PgPool,
        sink: S,
        merge_ids: M,
        cursor_key: [u8; 32],
    ) -> Result<Self, StorageFailure> {
        let store = Self {
            pool,
            retry: SerializableRetry::default(),
            directory: Arc::new(DirectoryCache::default()),
            sink: Arc::new(sink),
            merge_ids: Arc::new(merge_ids),
            cursor_key,
        };
        store.reload_directory().await?;
        Ok(store)
    }

    /// Use `retry` for every write transaction.
    pub fn with_retry(mut self, retry: SerializableRetry) -> Self {
        self.retry = retry;
        self
    }

    /// The pool the store reads and writes through.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Reload the merge table into the directory cache from the database.
    pub async fn reload_directory(&self) -> Result<(), StorageFailure> {
        let merged = load::merged_pairs(&self.pool).await?;
        self.directory.load(merged);
        Ok(())
    }

    /// Fold a merge or unmerge another node published into the directory
    /// cache. Call it before serving any read that follows the event.
    pub fn apply_event(&self, event: &crosstalk_spec::events::ingest::IngestEvent) {
        self.directory.apply(event);
    }

    /// `id`'s canonical agent and every agent merged into it, ascending,
    /// from the directory cache.
    pub fn members(&self, id: AgentId) -> Vec<AgentId> {
        self.directory.members(id)
    }
}

impl<S, M> AgentDirectory for PgAgents<S, M> {
    fn canonical(&self, id: AgentId) -> AgentId {
        self.directory.canonical(id)
    }
}
