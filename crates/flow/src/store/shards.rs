//! The correlator shards' tick checkpoints (`flow.shard_ticks`), read.
//!
//! Each shard records the last tick it processed; L7's frontier reads the
//! earliest of them as `PipelineFrontier::ticked_through` (`l5_flow`,
//! "Timing"). A record only moves forward. Its one writer is the
//! checkpoint ([`super::PgFlowDurability`]), which writes it in the
//! transaction that stores the shard's snapshot, so it never names a tick
//! the stored state has not run (`flow.checkpoint.ticks-with-state`,
//! INV-1216).

use crosstalk_spec::support::Timestamp;
use sqlx::PgPool;

use super::codec::timestamp;
use super::directory::ShardIndex;
use super::error::FlowStoreError;

/// The tick checkpoints of every correlator shard.
#[derive(Debug, Clone)]
pub struct PgShardTicks {
    pool: PgPool,
}

impl PgShardTicks {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The checkpoint of `shard`; `None` before its first tick.
    pub async fn of(&self, shard: ShardIndex) -> Result<Option<Timestamp>, FlowStoreError> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT ticked_through FROM flow.shard_ticks WHERE shard = $1")
                .bind(i32::from(shard.0))
                .fetch_optional(&self.pool)
                .await?;
        Ok(row
            .map(|(at,)| timestamp("shard_ticks.ticked_through", at))
            .transpose()?)
    }

    /// The earliest checkpoint over `shards` shards (`0..shards`); `None`
    /// while any of them has not ticked yet.
    pub async fn ticked_through(
        &self,
        shards: std::num::NonZeroU16,
    ) -> Result<Option<Timestamp>, FlowStoreError> {
        let (ticked, earliest): (i64, Option<i64>) = sqlx::query_as(
            "SELECT count(*), min(ticked_through) FROM flow.shard_ticks WHERE shard < $1",
        )
        .bind(i32::from(shards.get()))
        .fetch_one(&self.pool)
        .await?;
        if ticked < i64::from(shards.get()) {
            return Ok(None);
        }
        Ok(earliest
            .map(|at| timestamp("shard_ticks.ticked_through", at))
            .transpose()?)
    }
}
