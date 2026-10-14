//! [`PgFrontierSource`]: the pipeline's progress, read for L7's watermark
//! in Postgres mode (`FrontierSource`, `l7_topology`).
//!
//! ```text
//! ticked_through = PgShardTicks::ticked_through(shards)    the earliest checkpointed tick
//!                  (none yet ─▶ the epoch: nothing settles)
//! oldest_pending = min( oldest pending `at` and oldest dead letter `at` over the
//!                       pipeline groups (PgBus::group_stats),
//!                       the oldest `at` of a log entry a pipeline group takes but
//!                       has not admitted yet (seq past its admitted_through),
//!                       the oldest `at` still in the publish spool )
//! ```
//!
//! - A group admits log entries into its deliveries only when its consumer
//!   asks for the next one, so an envelope can sit in `transport.events`,
//!   past the group's `admitted_through`, with no delivery row: pending for
//!   the group, but not in `group_stats`. The frontier reads those entries
//!   itself (`UNADMITTED`, read-only on the `transport` schema: the bus has
//!   no read for it yet), so the watermark never passes an envelope a group
//!   has still to take (found by `integration::pg_frontier_covers_the_spool`:
//!   a drained spool's envelope was in the log, admitted by no one).
//!
//! - A dead letter in a pipeline group holds the watermark back on purpose
//!   (`topology.frontier.covers-pending`, INV-581).
//! - A spooled envelope is input not yet processed: the spool's oldest
//!   `at` bounds `oldest_pending` (`topology.frontier.covers-spool`,
//!   INV-1217), so after a database outage the watermark never finalizes
//!   a bucket a still-spooled capture belongs to.
//! - The flow group's deliveries stay pending until a checkpoint covers
//!   them (deferred acks), which holds the watermark back by at most one
//!   checkpoint interval.
//! - Exchanges in flight at the proxy are not counted, as in memory mode:
//!   the proxy keeps no registry of them, and their times are later than
//!   `now - settle_after` for any request shorter than the settle bound.
//!   `TransmissionClassified { cause: Refit }` has no producer yet, so no
//!   group needs excluding.
//!
//! Reads only; it lives in the composer because it reads L2, L5 and the
//! spool.

use std::num::NonZeroU16;

use crosstalk_flow::store::PgShardTicks;
use crosstalk_spec::aggregates::watermark::PipelineFrontier;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus};
use crosstalk_spec::interfaces::l7_topology::{EdgeError, FrontierSource};
use crosstalk_spec::support::Timestamp;
use crosstalk_store::sqlx::{self, PgPool};
use crosstalk_transport::{DrainTarget, GroupStats, PgBus, SpoolingBus};

/// Where the spool's backlog is read: its oldest unsent `at`.
pub trait SpoolBacklog: Send + Sync {
    fn oldest_spooled(&self) -> Option<Timestamp>;
}

impl<B: DrainTarget + EventBus + Send + Sync + 'static> SpoolBacklog for SpoolingBus<B> {
    fn oldest_spooled(&self) -> Option<Timestamp> {
        self.oldest_at()
    }
}

/// No spool (a process that never spools).
impl SpoolBacklog for () {
    fn oldest_spooled(&self) -> Option<Timestamp> {
        None
    }
}

/// The frontier over the bus, the shard ticks and the spool.
#[derive(Debug, Clone)]
pub struct PgFrontierSource<P> {
    bus: PgBus,
    /// The bus's database, for the entries no group admitted yet.
    pool: PgPool,
    ticks: PgShardTicks,
    shards: NonZeroU16,
    groups: Vec<ConsumerGroup>,
    spool: P,
}

impl<P: SpoolBacklog> PgFrontierSource<P> {
    /// The frontier of the pipeline whose correlator runs `shards` shards
    /// and whose consumers on the way to the edge store read with
    /// `groups`.
    pub fn new(
        bus: PgBus,
        pool: PgPool,
        ticks: PgShardTicks,
        shards: NonZeroU16,
        groups: Vec<ConsumerGroup>,
        spool: P,
    ) -> Self {
        Self {
            bus,
            pool,
            ticks,
            shards,
            groups,
            spool,
        }
    }
}

/// The frontier from its readings: the earliest shard tick (`None`
/// before every shard ticked), every group's backlog and its oldest
/// unadmitted log entry (only `groups` count), and the spool's oldest
/// unsent `at`.
pub fn combine(
    ticked_through: Option<Timestamp>,
    stats: &[GroupStats],
    unadmitted: &[(ConsumerGroup, Timestamp)],
    groups: &[ConsumerGroup],
    spooled: Option<Timestamp>,
) -> PipelineFrontier {
    let pending = stats
        .iter()
        .filter(|stats| groups.contains(&stats.group))
        .flat_map(|stats| [stats.oldest_pending, stats.oldest_dead_letter])
        .chain(
            unadmitted
                .iter()
                .filter(|(group, _)| groups.contains(group))
                .map(|(_, at)| Some(*at)),
        )
        .chain([spooled])
        .flatten()
        .min();
    PipelineFrontier {
        ticked_through: ticked_through.unwrap_or(Timestamp::from_micros(0)),
        oldest_pending: pending,
    }
}

fn read_failed(what: &str, error: impl std::fmt::Debug) -> EdgeError {
    EdgeError::Store {
        reason: format!("frontier: {what}: {error:?}"),
    }
}

/// Per group of `$1`, the earliest `at` of a routed log entry under one of
/// its subjects past its `admitted_through`: taken by the group, not yet
/// admitted into its deliveries.
const UNADMITTED: &str = "SELECT g.name, min(e.at) FROM transport.groups g \
     JOIN transport.events e ON e.seq > g.admitted_through \
         AND e.routed AND e.subject = ANY(g.subjects) \
     WHERE g.name = ANY($1) GROUP BY g.name";

/// Per group of `groups`, the oldest `at` of a log entry it takes but has
/// not admitted yet; groups with none are left out.
pub async fn unadmitted(
    pool: &PgPool,
    groups: &[ConsumerGroup],
) -> Result<Vec<(ConsumerGroup, Timestamp)>, EdgeError> {
    let names: Vec<String> = groups.iter().map(|group| group.0.clone()).collect();
    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(UNADMITTED)
        .bind(&names)
        .fetch_all(pool)
        .await
        .map_err(|error| read_failed("unadmitted log entries", error))?;
    Ok(rows
        .into_iter()
        .filter_map(|(name, at)| {
            at.map(|at| {
                (
                    ConsumerGroup(name),
                    Timestamp::from_micros(u64::try_from(at).unwrap_or(0)),
                )
            })
        })
        .collect())
}

impl<P: SpoolBacklog> FrontierSource for PgFrontierSource<P> {
    async fn frontier(&self) -> Result<PipelineFrontier, EdgeError> {
        // The spool first: an envelope drained between this read and the
        // log reads is then in the log, where they see it. Then the
        // unadmitted entries before the deliveries: an entry admitted
        // between the two reads is a delivery by the second.
        let spooled = self.spool.oldest_spooled();
        let unadmitted = unadmitted(&self.pool, &self.groups).await?;
        let ticked = self
            .ticks
            .ticked_through(self.shards)
            .await
            .map_err(|error| read_failed("shard ticks", error))?;
        let stats = self
            .bus
            .group_stats()
            .await
            .map_err(|error| read_failed("group stats", error))?;
        Ok(combine(ticked, &stats, &unadmitted, &self.groups, spooled))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(micros: u64) -> Timestamp {
        Timestamp::from_micros(micros)
    }

    fn stats(group: &str, pending: Option<u64>, dead: Option<u64>) -> GroupStats {
        GroupStats {
            group: ConsumerGroup(group.to_owned()),
            pending: u64::from(pending.is_some()),
            oldest_pending: pending.map(at),
            dead_letters: u64::from(dead.is_some()),
            oldest_dead_letter: dead.map(at),
        }
    }

    fn groups(names: &[&str]) -> Vec<ConsumerGroup> {
        names
            .iter()
            .map(|name| ConsumerGroup((*name).to_owned()))
            .collect()
    }

    #[test]
    fn nothing_pending_is_the_tick_alone() {
        let frontier = combine(
            Some(at(50)),
            &[stats("a", None, None)],
            &[],
            &groups(&["a"]),
            None,
        );
        assert_eq!(
            frontier,
            PipelineFrontier {
                ticked_through: at(50),
                oldest_pending: None
            }
        );
    }

    #[test]
    fn no_tick_yet_settles_nothing() {
        let frontier = combine(None, &[], &[], &groups(&["a"]), None);
        assert_eq!(frontier.ticked_through, at(0));
    }

    #[test]
    fn pending_and_dead_letters_of_pipeline_groups_count() {
        let all = [
            stats("a", Some(40), None),
            stats("b", None, Some(30)),
            stats("outside", Some(10), Some(5)),
        ];
        let frontier = combine(Some(at(50)), &all, &[], &groups(&["a", "b"]), None);
        assert_eq!(frontier.oldest_pending, Some(at(30)));
    }

    /// `topology.frontier.covers-spool`: the oldest spooled `at` bounds
    /// `oldest_pending`, whatever the groups hold.
    #[test]
    fn the_spool_bounds_oldest_pending() {
        for (pending, spooled, expected) in [
            (None, Some(20), 20),
            (Some(40), Some(20), 20),
            (Some(10), Some(20), 10),
        ] {
            let frontier = combine(
                Some(at(50)),
                &[stats("a", pending, None)],
                &[],
                &groups(&["a"]),
                spooled.map(at),
            );
            assert_eq!(frontier.oldest_pending, Some(at(expected)));
        }
    }
}
