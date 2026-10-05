//! The transactional outbox the flow stores publish through.
//!
//! **Choice: a transactional outbox, relayed after commit.** A write stages
//! the events it decided (`stage`) in `flow.outbox` inside its own
//! serializable transaction, so they commit or roll back with the change:
//! a refused or rolled-back write publishes nothing, and a retried
//! transaction stages its events once (the failed attempts' rows rolled
//! back with them). Once the transaction commits, the store relays every
//! staged event ([`Relay::relay`]) onto its [`EventSink`] in staging order
//! and deletes the rows in the same relay transaction.
//!
//! Delivery is at least once: a crash (or a lost connection) after the
//! commit and before the relay leaves the events staged, and the next relay
//! by any store on the database sends them; a relay whose own commit fails
//! after sending may send them again. Consumers of every flow event are
//! idempotent under redelivery (the bus already redelivers). A receiver that
//! re-queries the store on an event always sees the change, because nothing
//! is relayed before its transaction committed.
//!
//! Concurrent relays skip each other's locked rows (`FOR UPDATE SKIP
//! LOCKED`), so each staged event is sent by one of them. Events of two
//! concurrent writes may be relayed out of commit order; each event names
//! what changed and readers re-query, so no consumer depends on that order.

use crosstalk_spec::events::BusEvent;
use sqlx::{PgConnection, PgPool};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, warn};

use super::codec::{from_json, json};
use super::error::{Fault, FlowStoreError};

/// How many staged events one relay statement takes.
const RELAY_BATCH: i64 = 256;

/// Where relayed events go: the wiring stamps each into an `Envelope` and
/// publishes it on the bus.
#[derive(Debug, Clone)]
pub struct EventSink {
    sender: UnboundedSender<BusEvent>,
}

impl EventSink {
    pub fn new(sender: UnboundedSender<BusEvent>) -> Self {
        Self { sender }
    }
}

/// Stage `events` in the outbox, inside the caller's transaction.
pub(crate) async fn stage(conn: &mut PgConnection, events: &[BusEvent]) -> Result<(), Fault> {
    for event in events {
        sqlx::query("INSERT INTO flow.outbox (event) VALUES ($1)")
            .bind(json("outbox event", event)?)
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

/// Relays committed events from the outbox to a sink.
#[derive(Debug, Clone)]
pub struct Relay {
    pool: PgPool,
    sink: EventSink,
}

impl Relay {
    pub fn new(pool: PgPool, sink: EventSink) -> Self {
        Self { pool, sink }
    }

    /// Send every staged event, oldest first, and remove it. Returns how
    /// many were sent. When the sink's receiver is gone, nothing more is
    /// sent and the unsent events stay staged for a later relay.
    pub async fn relay(&self) -> Result<usize, FlowStoreError> {
        let mut sent = 0;
        loop {
            let batch = self.relay_batch().await?;
            sent += batch.sent;
            if batch.sent < batch.taken
                || i64::try_from(batch.taken).unwrap_or(i64::MAX) < RELAY_BATCH
            {
                return Ok(sent);
            }
        }
    }

    /// Relay after a committed write: a failure leaves the events staged
    /// for the next relay, so the write still succeeded; it is logged.
    pub(crate) async fn after_commit(&self) {
        if let Err(error) = self.relay().await {
            warn!(error = %error, "outbox relay failed; the events stay staged for the next relay");
        }
    }

    async fn relay_batch(&self) -> Result<Batch, FlowStoreError> {
        let mut tx = self.pool.begin().await?;
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT seq, event FROM flow.outbox ORDER BY seq \
             FOR UPDATE SKIP LOCKED LIMIT $1",
        )
        .bind(RELAY_BATCH)
        .fetch_all(&mut *tx)
        .await?;
        let taken = rows.len();
        let mut sent: Vec<i64> = Vec::with_capacity(taken);
        for (seq, text) in rows {
            let event: BusEvent = from_json("outbox event", &text)?;
            if self.sink.sender.send(event).is_err() {
                debug!(reason = "receiver closed", "outbox relay stopped");
                break;
            }
            sent.push(seq);
        }
        if !sent.is_empty() {
            // Exactly the rows this relay locked and sent: rows past a
            // closed receiver stay staged.
            sqlx::query("DELETE FROM flow.outbox WHERE seq = ANY($1)")
                .bind(&sent)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(Batch {
            taken,
            sent: sent.len(),
        })
    }
}

struct Batch {
    taken: usize,
    sent: usize,
}
