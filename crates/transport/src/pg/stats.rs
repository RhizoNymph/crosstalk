//! [`GroupStats`]: per group, what the bus still owes it. Read by the
//! composer's frontier (`PgFrontierSource`) and by `/healthz`; not a spec
//! read.

use crosstalk_spec::interfaces::l2_transport::{BusError, ConsumerGroup};
use crosstalk_spec::support::Timestamp;

use super::Shared;
use super::row::{bus_error, timestamp};

/// One group's backlog: its unacked deliveries and its dead letters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStats {
    pub group: ConsumerGroup,
    /// Deliveries admitted and not acked: ready, delayed or held.
    pub pending: u64,
    /// The earliest `Envelope::at` among them.
    pub oldest_pending: Option<Timestamp>,
    pub dead_letters: u64,
    /// The earliest `Envelope::at` among the dead letters.
    pub oldest_dead_letter: Option<Timestamp>,
}

const STATS: &str = "\
WITH pending AS ( \
    SELECT group_name, count(*) AS n, min(at) AS oldest \
    FROM transport.deliveries GROUP BY group_name), \
letters AS ( \
    SELECT group_name, count(*) AS n, min(at) AS oldest \
    FROM transport.dead_letters GROUP BY group_name), \
names AS ( \
    SELECT name AS group_name FROM transport.groups \
    UNION SELECT group_name FROM letters) \
SELECT n.group_name, coalesce(p.n, 0), p.oldest, coalesce(l.n, 0), l.oldest \
FROM names n \
LEFT JOIN pending p ON p.group_name = n.group_name \
LEFT JOIN letters l ON l.group_name = n.group_name \
ORDER BY n.group_name";

/// A stats row: group, pending, oldest pending, letters, oldest letter.
type StatsRow = (String, i64, Option<i64>, i64, Option<i64>);

/// Every group's stats, by name: every subscribed group, and every group
/// with a dead letter.
pub(crate) async fn group_stats(shared: &Shared) -> Result<Vec<GroupStats>, BusError> {
    let rows: Vec<StatsRow> = sqlx::query_as(STATS)
        .fetch_all(&shared.pool)
        .await
        .map_err(|e| bus_error("group stats", &e))?;
    Ok(rows
        .into_iter()
        .map(
            |(group, pending, oldest, letters, oldest_letter)| GroupStats {
                group: ConsumerGroup(group),
                pending: u64::try_from(pending).unwrap_or(0),
                oldest_pending: oldest.map(timestamp),
                dead_letters: u64::try_from(letters).unwrap_or(0),
                oldest_dead_letter: oldest_letter.map(timestamp),
            },
        )
        .collect())
}
