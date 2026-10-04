//! [`PgEdgeStore`]: the spec's `EdgeStore` on Postgres.
//!
//! Tables live in the `topology` schema (`migrations/`). Edge and access
//! buckets are range-partitioned on `bucket_start` ([`partition`]); the
//! contributions and accesses behind them are kept one row each, which is
//! what makes `apply` and `apply_access` idempotent and what the
//! drill-down and `FalseDetections::Exclude` read.
//!
//! - **Writes** ([`write`]) run in `READ COMMITTED` transactions that lock
//!   the one `state` row first: `FOR SHARE` for an apply, `FOR UPDATE` for
//!   activation, retention and the watermark. Every apply therefore sees
//!   the watermark, the active version and the dropped versions as the last
//!   committed control change left them, and no bucket a newly exposed
//!   watermark finalizes can change after it is exposed. Bucket counts are
//!   upserted with `+=`, so concurrent applies to one bucket lose nothing.
//! - **Events** the store decides (`TopicVersionActivated`,
//!   `WatermarkAdvanced`, `Changed::Watermark`, and traffic changes) are
//!   written to `topology.outbox` in the transaction that makes the change,
//!   and relayed to the bus after commit ([`crate::outbox`]).
//! - **Reads** ([`read`]) run in one `REPEATABLE READ READ ONLY`
//!   transaction: the watermark first, then the dropped versions, buckets,
//!   false detections and contributions, all from one snapshot. Rows are
//!   resolved through the environment and summed in Rust ([`fold`]).

mod drill;
mod edge_store;
pub mod error;
mod fold;
pub mod partition;
mod read;
mod write;

use std::num::NonZeroU64;
use std::sync::Arc;

use crosstalk_spec::aggregates::series::BucketWidth;
use crosstalk_spec::derived::flow::timing::CorrelationTiming;
use crosstalk_spec::support::{TimeWindow, Timestamp};
use crosstalk_store::{Layer, Migrations, StoreError};
use sqlx::PgPool;
use sqlx::migrate::Migrator;

use self::partition::{PartitionSpan, Partitions};
use crate::outbox::{OutboxRelay, Wake};

pub use self::error::DbError;
#[cfg(test)]
pub(crate) use self::fold::route_key as fold_route_key;

/// The topology layer's migrations, embedded.
pub static MIGRATIONS: Migrator = sqlx::migrate!("./migrations");

/// Run the topology layer's migrations in its own schema.
pub async fn migrate(pool: &PgPool) -> Result<(), StoreError> {
    crosstalk_store::migrate(pool, Layer::Topology, Migrations::Embedded(&MIGRATIONS)).await
}

/// The edge store's configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeStoreConfig {
    pub bucket_width: BucketWidth,
    /// The correlator's timing, whose `settle_after` the watermark trails
    /// the frontier by.
    pub timing: CorrelationTiming,
    /// How much time one bucket table partition covers.
    pub partition_span: PartitionSpan,
}

/// The aligned bucket of width `width` holding `at`; `None` only for an
/// instant in the last, partial bucket before `u64::MAX` microseconds.
pub fn bucket_of(width: BucketWidth, at: Timestamp) -> Option<TimeWindow> {
    let width = width.as_micros().get();
    let start = at.as_micros() - at.as_micros() % width;
    let end = start.checked_add(width)?;
    TimeWindow::new(Timestamp::from_micros(start), Timestamp::from_micros(end)).ok()
}

/// Whether both ends of `window` are bucket boundaries.
pub fn aligned(width: BucketWidth, window: TimeWindow) -> bool {
    width.is_boundary(window.start()) && width.is_boundary(window.end())
}

/// The Postgres edge store. Cloning shares the pool, the partition set and
/// the relay's wake-up.
pub struct PgEdgeStore<V> {
    pool: PgPool,
    config: EdgeStoreConfig,
    env: Arc<V>,
    partitions: Partitions,
    wake: Wake,
}

impl<V> Clone for PgEdgeStore<V> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config,
            env: Arc::clone(&self.env),
            partitions: self.partitions.clone(),
            wake: self.wake.clone(),
        }
    }
}

impl<V> PgEdgeStore<V> {
    /// A store over `pool` (migrated with [`migrate`]) reading `env`, and
    /// the relay that publishes what it commits. Spawn
    /// [`OutboxRelay::run`]; until it runs, committed events wait in the
    /// outbox.
    pub fn new(pool: PgPool, config: EdgeStoreConfig, env: V) -> (Self, OutboxRelay) {
        let (wake, relay) = OutboxRelay::new(pool.clone());
        let store = Self {
            pool,
            config,
            env: Arc::new(env),
            partitions: Partitions::new(config.partition_span),
            wake,
        };
        (store, relay)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn config(&self) -> EdgeStoreConfig {
        self.config
    }

    pub fn env(&self) -> &V {
        &self.env
    }

    fn width(&self) -> NonZeroU64 {
        self.config.bucket_width.as_micros()
    }
}
