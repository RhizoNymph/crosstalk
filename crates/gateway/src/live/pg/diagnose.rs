//! What a Postgres-mode process is waiting on, for a wait that runs out of
//! time ([`bounded`]) or an operator's question: the frontier against the
//! watermark and the frontier's parts (spool oldest, unadmitted entries
//! per group, group stats), the pipeline lock's holder, the spool, the
//! pool, the store outboxes and the recovery steps reached.
//!
//! Every part is read on its own; a part that cannot be read says why
//! instead of failing the whole report.

use std::fmt;
use std::future::Future;
use std::num::NonZeroU16;
use std::time::Duration;

use crosstalk_flow::store::PgShardTicks;
use crosstalk_spec::aggregates::watermark::PipelineFrontier;
use crosstalk_spec::interfaces::l2_transport::ConsumerGroup;
use crosstalk_spec::support::Timestamp;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_transport::{GroupStats, PgBus, SpoolStats};

use crate::live::frontier::{combine, unadmitted};
use crate::live::recovery::{PipelineStatus, StatusReader};
use crate::spool::LiveBus;
use crate::store::lock::PIPELINE_LOCK;

/// A read of one part, or why it failed.
pub type Part<T> = Result<T, String>;

/// Everything [`diagnose`] reads.
#[derive(Debug, Clone)]
pub struct PgDiagnosis {
    /// The frontier the watermark advances from, as `PgFrontierSource`
    /// computes it from the parts below.
    pub frontier: Part<PipelineFrontier>,
    /// The exposed watermark, when the caller has the edge store.
    pub watermark: Option<Part<Timestamp>>,
    /// The earliest shard tick (`None` until every shard ticked).
    pub ticked_through: Part<Option<Timestamp>>,
    /// The spool's oldest unsent `at`.
    pub spool_oldest: Option<Timestamp>,
    /// Per pipeline group, the oldest log entry it has not admitted.
    pub unadmitted: Part<Vec<(ConsumerGroup, Timestamp)>>,
    /// Every group's backlog and dead letters.
    pub groups: Part<Vec<GroupStats>>,
    /// The backend pids holding the pipeline advisory lock.
    pub lock_holders: Part<Vec<i32>>,
    /// The spool, for a process that spools.
    pub spool: Option<SpoolStats>,
    /// Rows waiting in the four store outboxes.
    pub outbox_rows: Part<i64>,
    /// Connections the pool holds, and how many of them are idle.
    pub pool_size: u32,
    pub pool_idle: usize,
    /// The pipeline's status, recovery steps included.
    pub status: Option<PipelineStatus>,
}

/// What [`diagnose`] reads from.
pub struct DiagnoseFrom<'a> {
    pub pool: &'a PgPool,
    pub bus: &'a PgBus,
    pub spool: Option<&'a LiveBus>,
    /// The pipeline groups the frontier counts.
    pub groups: &'a [ConsumerGroup],
    pub shards: NonZeroU16,
    pub status: Option<&'a StatusReader>,
    pub watermark: Option<Part<Timestamp>>,
}

fn reason(error: impl fmt::Debug) -> String {
    format!("{error:?}")
}

const OUTBOX_ROWS: &str = "SELECT (SELECT count(*) FROM reconstruct.outbox) \
     + (SELECT count(*) FROM flow.outbox) \
     + (SELECT count(*) FROM analysis.outbox) \
     + (SELECT count(*) FROM topology.outbox)";

/// Read every part.
pub async fn diagnose(from: DiagnoseFrom<'_>) -> PgDiagnosis {
    let spool_oldest = from.spool.and_then(LiveBus::oldest_at);
    let unadmitted = unadmitted(from.pool, from.groups).await.map_err(reason);
    let groups = from.bus.group_stats().await.map_err(reason);
    let ticked_through = PgShardTicks::new(from.pool.clone())
        .ticked_through(from.shards)
        .await
        .map_err(reason);
    let frontier = match (&ticked_through, &groups, &unadmitted) {
        (Ok(ticked), Ok(stats), Ok(unadmitted)) => Ok(combine(
            *ticked,
            stats,
            unadmitted,
            from.groups,
            spool_oldest,
        )),
        _ => Err("a part of the frontier did not read".to_owned()),
    };
    let lock_holders: Part<Vec<i32>> = sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND classid::bigint = $1 AND objid::bigint = $2",
    )
    .bind(PIPELINE_LOCK >> 32)
    .bind(PIPELINE_LOCK & 0xFFFF_FFFF)
    .fetch_all(from.pool)
    .await
    .map_err(reason);
    let outbox_rows: Part<i64> = sqlx::query_scalar(OUTBOX_ROWS)
        .fetch_one(from.pool)
        .await
        .map_err(reason);
    PgDiagnosis {
        frontier,
        watermark: from.watermark,
        ticked_through,
        spool_oldest,
        unadmitted,
        groups,
        lock_holders,
        spool: from.spool.map(LiveBus::stats),
        outbox_rows,
        pool_size: from.pool.size(),
        pool_idle: from.pool.num_idle(),
        status: from.status.map(StatusReader::snapshot),
    }
}

impl fmt::Display for PgDiagnosis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "frontier:        {:?}", self.frontier)?;
        writeln!(f, "watermark:       {:?}", self.watermark)?;
        writeln!(f, "ticked_through:  {:?}", self.ticked_through)?;
        writeln!(f, "spool oldest:    {:?}", self.spool_oldest)?;
        writeln!(f, "unadmitted:      {:?}", self.unadmitted)?;
        match &self.groups {
            Ok(groups) => {
                writeln!(f, "groups:")?;
                for group in groups {
                    writeln!(
                        f,
                        "  {}: pending {} (oldest {:?}), dead letters {} (oldest {:?})",
                        group.group.0,
                        group.pending,
                        group.oldest_pending,
                        group.dead_letters,
                        group.oldest_dead_letter
                    )?;
                }
            }
            Err(why) => writeln!(f, "groups:          unreadable: {why}")?,
        }
        writeln!(f, "lock holders:    {:?}", self.lock_holders)?;
        writeln!(f, "spool:           {:?}", self.spool)?;
        writeln!(f, "outbox rows:     {:?}", self.outbox_rows)?;
        writeln!(
            f,
            "pool:            {} connections, {} idle, {} in use",
            self.pool_size,
            self.pool_idle,
            usize::try_from(self.pool_size)
                .unwrap_or(usize::MAX)
                .saturating_sub(self.pool_idle)
        )?;
        match &self.status {
            Some(status) => writeln!(
                f,
                "status:          {} / recovery {} / lock {} / migrations {} / steps {:?}",
                status.pipeline_text(),
                status.recovery_text(),
                status.lock_text(),
                status.migrations_text(),
                status.steps
            ),
            None => writeln!(f, "status:          none"),
        }
    }
}

/// A wait that ran out of time, with what the process was waiting on.
#[derive(Debug, thiserror::Error)]
#[error("{what} did not finish within {waited:?}; the process was waiting on:\n{diagnosis}")]
pub struct Stalled {
    pub what: String,
    pub waited: Duration,
    pub diagnosis: Box<PgDiagnosis>,
}

/// Run `work` for at most `limit`; past it, read `diagnosis` and return
/// [`Stalled`] instead of waiting on.
pub async fn bounded<T, W, D>(
    limit: Duration,
    what: impl Into<String>,
    work: W,
    diagnosis: impl FnOnce() -> D,
) -> Result<T, Stalled>
where
    W: Future<Output = T>,
    D: Future<Output = PgDiagnosis>,
{
    match tokio::time::timeout(limit, work).await {
        Ok(done) => Ok(done),
        Err(_) => Err(Stalled {
            what: what.into(),
            waited: limit,
            diagnosis: Box::new(diagnosis().await),
        }),
    }
}
