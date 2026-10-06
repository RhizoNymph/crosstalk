//! Appending to the log: `publish`, the spool's batch publish, and the
//! connection probe.
//!
//! One transaction per call. It takes [`PUBLISH_LOCK`] (transaction
//! scoped), so publishes commit in `seq` order: a reader that sees `seq n`
//! committed has seen every committed `seq` below it, which is what lets
//! `next` advance a group's `admitted_through` past the log head it saw.
//! Each insert is `ON CONFLICT (id) DO NOTHING`
//! (`transport.publish.idempotent-on-id`), and a commit that added a row
//! notifies [`CHANNEL`].

use crosstalk_spec::events::Envelope;
use crosstalk_spec::interfaces::l2_transport::BusError;

use super::Shared;
use super::row::{EventRow, bus_error};

/// The `LISTEN`/`NOTIFY` channel a commit that added log entries notifies.
pub(crate) const CHANNEL: &str = "transport_events";

/// The transaction-scoped advisory lock every log append holds ("ctxpubl1"
/// in ASCII). Distinct from every other advisory key crosstalk takes.
pub(crate) const PUBLISH_LOCK: i64 = 0x6374_7870_7562_6C31;

/// Insert `rows` in order, in one transaction; returns how many were new.
async fn insert(shared: &Shared, rows: &[EventRow]) -> Result<u64, sqlx::Error> {
    let mut tx = shared.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(PUBLISH_LOCK)
        .execute(&mut *tx)
        .await?;
    let mut inserted = 0;
    for row in rows {
        let done = sqlx::query(
            "INSERT INTO transport.events (id, subject, at, envelope) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
        )
        .bind(&row.id)
        .bind(&row.subject)
        .bind(row.at)
        .bind(&row.envelope)
        .execute(&mut *tx)
        .await?;
        inserted += done.rows_affected();
    }
    if inserted > 0 {
        sqlx::query("SELECT pg_notify($1, '')")
            .bind(CHANNEL)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(inserted)
}

/// Publish `envelopes`, in order, in one transaction bounded by the
/// config's `publish_timeout`. `Ok` means every one is in the log (new or
/// already there). A timeout is [`BusError::Disconnected`]: whether the
/// commit landed is unknown, and a retry under the same ids is harmless.
pub(crate) async fn publish(shared: &Shared, envelopes: &[Envelope]) -> Result<(), BusError> {
    let rows = envelopes
        .iter()
        .map(EventRow::encode)
        .collect::<Result<Vec<_>, _>>()?;
    let limit = shared.config.publish_timeout.get();
    match tokio::time::timeout(limit, insert(shared, &rows)).await {
        Ok(Ok(inserted)) => {
            if inserted > 0 {
                shared.wake_all();
            }
            tracing::debug!(
                envelopes = rows.len(),
                inserted,
                first = rows.first().map(|r| r.id.as_str()),
                "published to the log"
            );
            Ok(())
        }
        Ok(Err(error)) => {
            let mapped = bus_error("publish", &error);
            tracing::warn!(envelopes = rows.len(), error = ?mapped, "publish failed");
            Err(mapped)
        }
        Err(_) => {
            tracing::warn!(
                envelopes = rows.len(),
                timeout_ms = u64::try_from(limit.as_millis()).unwrap_or(u64::MAX),
                "publish timed out; reporting the database unreachable"
            );
            Err(BusError::Disconnected)
        }
    }
}

/// Whether the database answers, within the publish timeout.
pub(crate) async fn probe(shared: &Shared) -> Result<(), BusError> {
    let limit = shared.config.publish_timeout.get();
    let ping = sqlx::query("SELECT 1").execute(&shared.pool);
    match tokio::time::timeout(limit, ping).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(bus_error("probe", &error)),
        Err(_) => Err(BusError::Disconnected),
    }
}
