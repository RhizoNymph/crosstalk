//! Range partitions of the bucket tables (decision D3: plain Postgres with
//! partitioned bucket tables, no TimescaleDB).
//!
//! `edge_buckets` and `access_buckets` are partitioned by range on
//! `bucket_start`. Each partition covers one [`PartitionSpan`] aligned to
//! the epoch, named `<table>_p<start micros>`, and is created on demand
//! before the first row of its range is written, so the parent never needs
//! a default partition. Creating one is idempotent (`IF NOT EXISTS`, and a
//! lost race on the catalog's unique index counts as created), and each
//! process remembers what it made so the hot path skips the DDL after the
//! first write of a range. Old partitions can later be detached and dropped
//! whole by a time-based retention job; version retention deletes rows.

use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_store::DbFailure;
use sqlx::AssertSqlSafe;
use sqlx::PgPool;

use super::error::DbError;

/// How much time one partition of a bucket table covers, in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionSpan(NonZeroU64);

impl PartitionSpan {
    /// One day: 1 + (one day in microseconds - 1), non-zero by construction.
    pub const DAY: Self = Self(NonZeroU64::MIN.saturating_add(86_400_000_000 - 1));

    pub const fn from_micros(micros: NonZeroU64) -> Self {
        Self(micros)
    }

    pub const fn as_micros(self) -> NonZeroU64 {
        self.0
    }
}

impl Default for PartitionSpan {
    fn default() -> Self {
        Self::DAY
    }
}

/// A partitioned bucket table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Bucketed {
    Edges,
    Accesses,
}

impl Bucketed {
    fn parent(self) -> &'static str {
        match self {
            Self::Edges => "topology.edge_buckets",
            Self::Accesses => "topology.access_buckets",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Edges => "topology.edge_buckets_p",
            Self::Accesses => "topology.access_buckets_p",
        }
    }
}

/// The partitions this process knows exist. Cloning shares the set.
#[derive(Debug, Clone)]
pub struct Partitions {
    span: PartitionSpan,
    made: Arc<Mutex<BTreeSet<(Bucketed, i64)>>>,
}

impl Partitions {
    pub fn new(span: PartitionSpan) -> Self {
        Self {
            span,
            made: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    /// The range `[start, end)` of the partition holding `bucket_start`.
    pub fn range_of(&self, bucket_start: i64) -> Result<(i64, i64), DbError> {
        let span = i64::try_from(self.span.as_micros().get())
            .map_err(|_| DbError::inconsistent("partition span does not fit a bigint"))?;
        let start = bucket_start - bucket_start.rem_euclid(span);
        let end = start
            .checked_add(span)
            .ok_or_else(|| DbError::inconsistent("partition end past bigint"))?;
        Ok((start, end))
    }

    fn known(&self, key: (Bucketed, i64)) -> bool {
        self.made
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&key)
    }

    fn remember(&self, key: (Bucketed, i64)) {
        self.made
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key);
    }

    /// Make sure the partition of `table` holding `bucket_start` exists.
    pub async fn ensure(
        &self,
        pool: &PgPool,
        table: Bucketed,
        bucket_start: i64,
    ) -> Result<(), DbError> {
        let (start, end) = self.range_of(bucket_start)?;
        if self.known((table, start)) {
            return Ok(());
        }
        // Spliced: only the fixed table names and two integers.
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {prefix}{start} PARTITION OF {parent} FOR VALUES FROM ({start}) TO ({end})",
            prefix = table.prefix(),
            parent = table.parent(),
        );
        match sqlx::query(AssertSqlSafe(ddl)).execute(pool).await {
            Ok(_) => {}
            Err(error) => {
                let error = DbError::from(error);
                let lost_race = match error.failure() {
                    Some(DbFailure::UniqueViolation { .. }) => true,
                    Some(DbFailure::Server { sqlstate }) => sqlstate.as_deref() == Some("42P07"),
                    _ => false,
                };
                if !lost_race {
                    return Err(error);
                }
            }
        }
        tracing::debug!(table = table.parent(), start, end, "bucket partition ready");
        self.remember((table, start));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_are_aligned_to_the_span() {
        let partitions = Partitions::new(PartitionSpan::from_micros(
            NonZeroU64::new(100).expect("non-zero"),
        ));
        assert_eq!(partitions.range_of(0).ok(), Some((0, 100)));
        assert_eq!(partitions.range_of(99).ok(), Some((0, 100)));
        assert_eq!(partitions.range_of(100).ok(), Some((100, 200)));
        assert_eq!(partitions.range_of(250).ok(), Some((200, 300)));
    }
}
