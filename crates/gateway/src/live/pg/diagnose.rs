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
    /// Every group's deliveries by state (`ready`, `held`, `delayed`): a
    /// `held` one nobody settles is a consumer that lost it.
    pub deliveries: Part<Vec<(String, String, i64)>>,
    /// The oldest pending deliveries themselves: group, seq, state,
    /// attempt, how far `available_at` is from the bus clock (micros), the
    /// last error and the subject.
    pub pending_rows: Part<Vec<PendingRow>>,
    /// Sessions that could block a take or a reaping: idle in a
    /// transaction, waiting on a lock, or holding a lock on the bus's
    /// `deliveries` or `groups` tables.
    pub blockers: Part<Vec<String>>,
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

/// One pending delivery, for [`PgDiagnosis::pending_rows`].
pub type PendingRow = (
    String,
    i64,
    String,
    i32,
    Option<i64>,
    Option<String>,
    String,
);

const PENDING_ROWS: &str = "SELECT d.group_name, d.seq, d.state, d.attempt, \
     d.available_at - $1, d.last_error, e.subject \
     FROM transport.deliveries d JOIN transport.events e ON e.seq = d.seq \
     ORDER BY d.seq LIMIT 20";

const BLOCKERS: &str = "SELECT format('pid %s (%s, port %s) %s xact since %s wait %s/%s locks [%s] query %s', \
       a.pid, a.application_name, a.client_port, a.state, a.xact_start, a.wait_event_type, a.wait_event, \
       (SELECT string_agg(DISTINCT c.relname || ':' || l.mode, ', ') FROM pg_locks l \
          JOIN pg_class c ON c.oid = l.relation \
          WHERE l.pid = a.pid AND c.relname IN ('deliveries', 'groups', 'events')), \
       left(a.query, 100)) \
     FROM pg_stat_activity a \
     WHERE a.datname = current_database() AND a.pid <> pg_backend_pid() \
       AND (a.state LIKE 'idle in transaction%' OR a.wait_event_type = 'Lock' \
            OR EXISTS (SELECT 1 FROM pg_locks l JOIN pg_class c ON c.oid = l.relation \
                       WHERE l.pid = a.pid AND c.relname IN ('deliveries', 'groups'))) \
     ORDER BY a.xact_start NULLS LAST";

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
    let deliveries: Part<Vec<(String, String, i64)>> = sqlx::query_as(
        "SELECT group_name, state, count(*) FROM transport.deliveries \
         GROUP BY group_name, state ORDER BY group_name, state",
    )
    .fetch_all(from.pool)
    .await
    .map_err(reason);
    let now = i64::try_from(super::bus_clock().now().as_micros()).unwrap_or(i64::MAX);
    let pending_rows: Part<Vec<PendingRow>> = sqlx::query_as(PENDING_ROWS)
        .bind(now)
        .fetch_all(from.pool)
        .await
        .map_err(reason);
    let blockers: Part<Vec<String>> = sqlx::query_scalar(BLOCKERS)
        .fetch_all(from.pool)
        .await
        .map_err(reason);
    PgDiagnosis {
        deliveries,
        pending_rows,
        blockers,
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

impl PgDiagnosis {
    /// What `Live::settle` (and a drain at shutdown) still waits on, as
    /// `PgQuiet::busy` decides it: a live slot's group holding a delivery,
    /// a record in the spool, a row in a store outbox. Empty when none of
    /// those is the case (a settle then waits on the stages' own activity).
    pub fn settle_waits_on(&self) -> Vec<String> {
        let mut waits = Vec::new();
        let slots: Vec<ConsumerGroup> = crate::live::Slot::ALL
            .iter()
            .map(|slot| slot.group())
            .collect();
        match &self.groups {
            Ok(groups) => {
                for group in groups
                    .iter()
                    .filter(|group| group.pending > 0 && slots.contains(&group.group))
                {
                    let states = match &self.deliveries {
                        Ok(rows) => rows
                            .iter()
                            .filter(|(name, _, _)| *name == group.group.0)
                            .map(|(_, state, count)| format!("{count} {state}"))
                            .collect::<Vec<_>>()
                            .join(", "),
                        Err(_) => "states unreadable".to_owned(),
                    };
                    waits.push(format!(
                        "group {} holds {} deliveries ({states})",
                        group.group.0, group.pending
                    ));
                }
            }
            Err(why) => waits.push(format!("the groups did not read: {why}")),
        }
        if let Some(spool) = &self.spool
            && spool.records > 0
        {
            waits.push(format!("the spool holds {} records", spool.records));
        }
        match &self.outbox_rows {
            Ok(0) => {}
            Ok(rows) => waits.push(format!("the store outboxes hold {rows} rows")),
            Err(why) => waits.push(format!("the outboxes did not read: {why}")),
        }
        waits
    }
}

impl fmt::Display for PgDiagnosis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.settle_waits_on() {
            waits if waits.is_empty() => writeln!(
                f,
                "settle waits on: nothing the bus, spool or outboxes hold (a stage's own work)"
            )?,
            waits => writeln!(f, "settle waits on: {}", waits.join("; "))?,
        }
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
        writeln!(f, "deliveries:      {:?}", self.deliveries)?;
        match &self.pending_rows {
            Ok(rows) => {
                for (group, seq, state, attempt, due, error, subject) in rows {
                    writeln!(
                        f,
                        "  pending {group} seq {seq} {subject}: {state}, attempt {attempt}, \
                         due in {due:?} micros, last error {error:?}"
                    )?;
                }
            }
            Err(why) => writeln!(f, "  pending rows unreadable: {why}")?,
        }
        match &self.blockers {
            Ok(blockers) if blockers.is_empty() => writeln!(f, "blockers:        none")?,
            Ok(blockers) => {
                for blocker in blockers {
                    writeln!(f, "blocker:         {blocker}")?;
                }
            }
            Err(why) => writeln!(f, "blockers:        unreadable: {why}")?,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::Slot;

    fn stats(group: ConsumerGroup, pending: u64) -> GroupStats {
        GroupStats {
            group,
            pending,
            oldest_pending: None,
            dead_letters: 0,
            oldest_dead_letter: None,
        }
    }

    fn diagnosis(groups: Vec<GroupStats>, outbox: i64) -> PgDiagnosis {
        PgDiagnosis {
            frontier: Err("not read".to_owned()),
            watermark: None,
            ticked_through: Ok(None),
            spool_oldest: None,
            unadmitted: Ok(Vec::new()),
            groups: Ok(groups),
            lock_holders: Ok(Vec::new()),
            spool: None,
            outbox_rows: Ok(outbox),
            deliveries: Ok(vec![(Slot::SurfaceRelay.group().0, "held".to_owned(), 1)]),
            pending_rows: Ok(Vec::new()),
            blockers: Ok(Vec::new()),
            pool_size: 0,
            pool_idle: 0,
            status: None,
        }
    }

    /// The surface relay is a live slot the frontier does not count, so
    /// its held delivery is named only as what a settle waits on; a group
    /// outside the live slots (the exchange log) is not.
    #[test]
    fn settle_waits_on_names_a_live_slot_group_holding_a_delivery() {
        let held = diagnosis(
            vec![
                stats(Slot::L3Reconstruct.group(), 0),
                stats(Slot::SurfaceRelay.group(), 1),
                stats(ConsumerGroup("exchange-log".to_owned()), 4),
            ],
            2,
        );
        let waits = held.settle_waits_on();
        assert_eq!(waits.len(), 2, "{waits:?}");
        assert!(
            waits[0].contains("live-surface-relay") && waits[0].contains("1 held"),
            "{waits:?}"
        );
        assert!(waits[1].contains("2 rows"), "{waits:?}");
        assert!(
            held.to_string()
                .starts_with("settle waits on: group live-surface-relay")
        );
        let quiet = diagnosis(vec![stats(Slot::L5Flow.group(), 0)], 0);
        assert!(quiet.settle_waits_on().is_empty());
    }
}
