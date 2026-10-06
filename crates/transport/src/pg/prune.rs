//! Log retention: [`PgBus::prune`](super::PgBus::prune).
//!
//! An envelope is pruned once every group is done with it and it is older
//! than the retention (decision Q6). A group is done with every `seq` below
//! its horizon: its lowest pending delivery, or `admitted_through + 1` when
//! it has none. Envelopes a delivery or a dead letter names are always
//! kept (dead letters are never dropped automatically, and a replay needs
//! the envelope's log entry). With no group at all, only age decides.

use crosstalk_spec::interfaces::l2_transport::BusError;
use crosstalk_spec::support::Timestamp;
use std::time::Duration;

use super::Shared;
use super::row::{bus_error, duration_micros, micros};

const PRUNE: &str = "\
WITH horizon AS ( \
    SELECT min(coalesce(p.lowest, g.admitted_through + 1)) AS seq \
    FROM transport.groups g \
    LEFT JOIN (SELECT group_name, min(seq) AS lowest \
               FROM transport.deliveries GROUP BY group_name) p \
      ON p.group_name = g.name) \
DELETE FROM transport.events e \
WHERE e.seq < coalesce((SELECT seq FROM horizon), 9223372036854775807) \
  AND e.at < $1 \
  AND NOT EXISTS (SELECT 1 FROM transport.deliveries d WHERE d.seq = e.seq) \
  AND NOT EXISTS (SELECT 1 FROM transport.dead_letters l WHERE l.seq = e.seq)";

/// Delete log entries every group is done with whose `at` is more than
/// `keep` before `now`; returns how many.
pub(crate) async fn prune(
    shared: &Shared,
    now: Timestamp,
    keep: Duration,
) -> Result<u64, BusError> {
    let cutoff = micros(now)?.saturating_sub(duration_micros(keep));
    let done = sqlx::query(PRUNE)
        .bind(cutoff)
        .execute(&shared.pool)
        .await
        .map_err(|e| bus_error("prune", &e))?;
    let pruned = done.rows_affected();
    tracing::info!(pruned, cutoff_micros = cutoff, "bus log pruned");
    Ok(pruned)
}
