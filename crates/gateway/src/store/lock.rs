//! The pipeline lock: one pipeline process per database
//! (`docs/features/postgres_stores.md`, "Single pipeline process").
//!
//! `serve` (roles that run a pipeline) takes a session advisory lock,
//! `pg_try_advisory_lock(PIPELINE_LOCK)`, on a connection of its own,
//! outside the pool, and holds it for its lifetime. Because of it the bus
//! can hand back deliveries a stopped process held, and the correlator can
//! assume it is the only writer of its checkpoint. A second process finds
//! the lock taken and stays not ready (`pipeline_lock: held elsewhere`),
//! consuming and relaying nothing.
//!
//! The holder pings its connection every [`PING`]. A failed ping means the
//! session (and so the lock) may be gone: [`PipelineLock::lost`] resolves
//! and the pipeline stops, since another process may take the lock now.
//! Dropping the lock closes the connection, which releases it.

use std::time::Duration;

use crosstalk_store::sqlx::postgres::PgConnection;
use crosstalk_store::sqlx::{self, Connection};
use crosstalk_store::{DatabaseUrl, StoreError};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The advisory lock key (`crosstal` in ASCII).
pub const PIPELINE_LOCK: i64 = 0x6372_6f73_7374_616c;

/// How often the holder checks its session.
pub const PING: Duration = Duration::from_secs(5);

/// The held lock: a task owning the connection. Dropped, it releases.
#[derive(Debug)]
pub struct PipelineLock {
    holder: JoinHandle<()>,
    lost: Option<oneshot::Receiver<String>>,
}

impl Drop for PipelineLock {
    fn drop(&mut self) {
        self.holder.abort();
    }
}

impl PipelineLock {
    /// Try to take the lock on a new connection to `url`. `None` when
    /// another session holds it.
    pub async fn try_take(url: &DatabaseUrl) -> Result<Option<Self>, StoreError> {
        let mut connection = PgConnection::connect_with(url.connect_options()).await?;
        let (taken,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(PIPELINE_LOCK)
            .fetch_one(&mut connection)
            .await?;
        if !taken {
            // Closing politely or not, the session never held the lock.
            let _ = connection.close().await;
            return Ok(None);
        }
        tracing::info!("pipeline lock taken");
        let (lost_tx, lost) = oneshot::channel();
        let holder = tokio::spawn(hold(connection, lost_tx));
        Ok(Some(Self {
            holder,
            lost: Some(lost),
        }))
    }

    /// Resolves with why once the lock may be lost (its session failed a
    /// ping). Pending forever after the first call.
    pub async fn lost(&mut self) -> String {
        match self.lost.take() {
            Some(lost) => lost
                .await
                .unwrap_or_else(|_| "the lock holder stopped".to_owned()),
            None => std::future::pending().await,
        }
    }
}

/// Keep `connection` alive, pinging it every [`PING`]; report the first
/// failure and end (dropping the connection).
async fn hold(mut connection: PgConnection, lost: oneshot::Sender<String>) {
    let mut ticker = tokio::time::interval(PING);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if let Err(error) = connection.ping().await {
            let why = crosstalk_store::classify(&error).to_string();
            tracing::error!(error = %why, "pipeline lock session failed; the pipeline stops");
            let _ = lost.send(why);
            return;
        }
    }
}
