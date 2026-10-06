//! The agent store's outbox and its relay
//! (`reconstruct.outbox.stable-envelope-id`, INV-1211).
//!
//! A write stages its events with [`stage`] in the transaction that makes
//! the change (migration `0001_agents`). After the commit, [`relay`]:
//!
//! 1. takes the rows (`FOR UPDATE SKIP LOCKED`, in `seq` order) in a
//!    transaction of its own, stamps every row that has no envelope id yet
//!    with the sink's [`EventSink::stamp`] (migration `0005_outbox_ids`)
//!    and commits, so the id and time are fixed before the first publish;
//! 2. publishes each row as an envelope under its stamp, in `seq` order,
//!    awaiting the sink;
//! 3. deletes the rows it published.
//!
//! A failure or a stop after step 1 leaves the rows stamped; the next relay
//! publishes them under the same ids, which a bus idempotent on ids holds
//! once. A relay reads only committed rows, so nothing is published before
//! the transaction that staged it committed.

use crosstalk_spec::events::{BusEvent, Envelope};
use sqlx::{PgConnection, PgPool};

use super::codec::{from_json, id_of, id_text, json, micros, timestamp};
use crate::error::{StorageFailure, TxFailure};
use crate::publish::{EventSink, Stamp};

/// How many rows one relay pass takes.
const BATCH: i64 = 256;

/// Append `events` to the outbox in `conn`'s transaction, unstamped,
/// returning their rows.
pub(super) async fn stage(
    conn: &mut PgConnection,
    events: &[BusEvent],
) -> Result<Vec<i64>, TxFailure> {
    let mut seqs = Vec::with_capacity(events.len());
    for event in events {
        let (seq,): (i64,) =
            sqlx::query_as("INSERT INTO reconstruct.outbox (event) VALUES ($1) RETURNING seq")
                .bind(json(event)?)
                .fetch_one(&mut *conn)
                .await?;
        seqs.push(seq);
    }
    Ok(seqs)
}

/// Which outbox rows a relay takes.
#[derive(Debug, Clone, Copy)]
pub(super) enum Rows<'a> {
    /// The rows one committed write staged.
    Staged(&'a [i64]),
    /// Every row, oldest first, in batches until none is left.
    All,
}

/// An outbox row: its sequence, event and stamp columns.
type OutboxRow = (i64, String, Option<String>, Option<i64>);

/// A taken row, stamped.
struct Taken {
    seq: i64,
    envelope: Envelope,
}

/// The stamp a row holds, or `None` when it has none yet.
fn stored_stamp(
    seq: i64,
    id: Option<String>,
    at: Option<i64>,
) -> Result<Option<Stamp>, StorageFailure> {
    match (id, at) {
        (Some(id), Some(at)) => Ok(Some(Stamp {
            id: id_of("outbox.envelope_id", &id)?,
            at: timestamp("outbox.at", at)?,
        })),
        (None, None) => Ok(None),
        // The `outbox_stamped` constraint refuses a half stamp.
        _ => Err(StorageFailure::Inconsistent {
            reason: format!("outbox row {seq} is half stamped"),
        }),
    }
}

/// Step 1: take the next batch of `rows`, stamp the unstamped ones and
/// commit the stamps.
async fn take<S: EventSink>(
    pool: &PgPool,
    sink: &S,
    rows: Rows<'_>,
) -> Result<Vec<Taken>, StorageFailure> {
    let mut tx = pool.begin().await?;
    let found: Vec<OutboxRow> = match rows {
        Rows::Staged(seqs) => {
            sqlx::query_as(
                "SELECT seq, event, envelope_id, at FROM reconstruct.outbox \
                 WHERE seq = ANY($1) ORDER BY seq FOR UPDATE SKIP LOCKED",
            )
            .bind(seqs)
            .fetch_all(&mut *tx)
            .await?
        }
        Rows::All => {
            sqlx::query_as(
                "SELECT seq, event, envelope_id, at FROM reconstruct.outbox \
                 ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED",
            )
            .bind(BATCH)
            .fetch_all(&mut *tx)
            .await?
        }
    };
    let mut taken = Vec::with_capacity(found.len());
    for (seq, event, id, at) in found {
        let stamp = match stored_stamp(seq, id, at)? {
            Some(stamp) => stamp,
            None => {
                let stamp = sink.stamp()?;
                sqlx::query(
                    "UPDATE reconstruct.outbox SET envelope_id = $2, at = $3 \
                     WHERE seq = $1 AND envelope_id IS NULL",
                )
                .bind(seq)
                .bind(id_text(stamp.id))
                .bind(micros(stamp.at)?)
                .execute(&mut *tx)
                .await?;
                stamp
            }
        };
        taken.push(Taken {
            seq,
            envelope: Envelope {
                id: stamp.id,
                at: stamp.at,
                event: from_json("outbox.event", &event)?,
            },
        });
    }
    tx.commit().await?;
    Ok(taken)
}

/// Relay `rows`: stamp, publish, delete. Returns how many events were
/// published; a sink failure returns it after deleting what was published
/// before it, leaving the rest for the next relay.
pub(super) async fn relay<S: EventSink>(
    pool: &PgPool,
    sink: &S,
    rows: Rows<'_>,
) -> Result<usize, StorageFailure> {
    let mut total = 0;
    loop {
        let taken = take(pool, sink, rows).await?;
        if taken.is_empty() {
            return Ok(total);
        }
        let mut published = Vec::with_capacity(taken.len());
        let mut failed = None;
        for row in taken {
            let id = row.envelope.id;
            match sink.publish(row.envelope).await {
                Ok(()) => published.push(row.seq),
                Err(error) => {
                    tracing::warn!(seq = row.seq, envelope = %id.ulid_text(), error = %error, "outbox event not published; left stamped for the next relay");
                    failed = Some(error);
                    break;
                }
            }
        }
        if !published.is_empty() {
            sqlx::query("DELETE FROM reconstruct.outbox WHERE seq = ANY($1)")
                .bind(&published)
                .execute(pool)
                .await?;
        }
        total += published.len();
        if let Some(error) = failed {
            return Err(error.into());
        }
        if matches!(rows, Rows::Staged(_)) {
            return Ok(total);
        }
    }
}
