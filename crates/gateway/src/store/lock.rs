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
//! The holder pings its connection every [`PING`], and each ping must
//! answer within [`PING_TIMEOUT`]. A failed ping, or one that does not
//! answer in time, means the session (and so the lock) may be gone:
//! [`PipelineLock::lost`] resolves with a [`LockLoss`] naming which, and
//! the pipeline's incarnation is abandoned, since another process may take
//! the lock now; the process takes the lock again once it is free. The
//! holder reports first, then drops the connection without a polite close
//! (a close would wait on the same dead link).
//!
//! # The detection bound (INV-1236)
//!
//! A silent network partition answers nothing and resets nothing, so
//! without a deadline a ping waits until TCP gives up (minutes) or the
//! link heals. The server meanwhile reaps the partitioned session, and
//! with it the advisory lock, after [`SERVER_REAP`] (deploy's
//! `tcp_keepalives_idle=10`, `tcp_keepalives_interval=5`,
//! `tcp_keepalives_count=3`, `tcp_user_timeout=25000`: about 25 s). From
//! then on another process may take the lock while this one still runs
//! its pipeline. So the holder must notice first: a partition that starts
//! just after a ping answered is caught by the next ping, [`PING`] later,
//! timing out [`PING_TIMEOUT`] after that. [`DETECTION`], their sum, is
//! the worst case, and a compile-time assertion keeps it strictly below
//! [`SERVER_REAP`]. Lengthen [`PING`] or [`PING_TIMEOUT`], or shorten the
//! server's keepalives, only together.
//!
//! Taking the lock is bounded too ([`TAKE_TIMEOUT`]): a connect or
//! `pg_try_advisory_lock` across a partition fails as
//! [`TakeError::TimedOut`] instead of hanging the pipeline's start.

use std::time::Duration;

use crosstalk_store::sqlx::postgres::PgConnection;
use crosstalk_store::sqlx::{self, Connection};
use crosstalk_store::{DatabaseUrl, DbFailure, StoreError};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::live::recovery::LossReason;

/// The advisory lock key (`crosstal` in ASCII).
pub const PIPELINE_LOCK: i64 = 0x6372_6f73_7374_616c;

/// How often the holder checks its session.
pub const PING: Duration = Duration::from_secs(5);

/// How long one ping may wait for its answer before the session counts as
/// failed. Shorter than [`PING`], so pings never overlap.
pub const PING_TIMEOUT: Duration = Duration::from_secs(4);

/// The worst case from a silent partition of the lock's session to
/// [`PipelineLock::lost`] resolving: one [`PING`] interval, then a ping
/// that times out.
pub const DETECTION: Duration = PING.saturating_add(PING_TIMEOUT);

/// How long the server keeps a silently partitioned session, and its
/// advisory lock, with deploy's settings: `tcp_keepalives_idle` 10 s plus
/// `tcp_keepalives_interval` 5 s × `tcp_keepalives_count` 3, the same as
/// `tcp_user_timeout` 25000 ms (`docs/features/deploy.md`).
pub const SERVER_REAP: Duration = Duration::from_secs(25);

/// How long taking the lock (connect and `pg_try_advisory_lock`) may take.
pub const TAKE_TIMEOUT: Duration = Duration::from_secs(10);

// INV-1236: the holder notices a lost session strictly before the server
// can hand the lock to another process.
const _: () = assert!(DETECTION.as_millis() < SERVER_REAP.as_millis());
const _: () = assert!(PING_TIMEOUT.as_millis() < PING.as_millis());

/// Why the lock may be lost.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LockLoss {
    /// A ping failed: the connection reset, closed, or the server refused.
    #[error("the lock's session failed a ping: {failure}")]
    PingFailed { failure: DbFailure },
    /// A ping did not answer within [`PING_TIMEOUT`]: a silent partition,
    /// or a server too stalled to answer.
    #[error("the lock's session did not answer a ping within {after:?}")]
    PingTimedOut { after: Duration },
    /// The holder task ended without reporting (it panicked).
    #[error("the lock holder stopped")]
    HolderStopped,
}

impl LockLoss {
    /// A stable label for logs and metrics.
    pub fn reason(&self) -> &'static str {
        self.kind().label()
    }

    /// Which kind of loss, without its detail: what the recovery report
    /// keeps.
    pub fn kind(&self) -> LossReason {
        match self {
            Self::PingFailed { .. } => LossReason::PingFailed,
            Self::PingTimedOut { .. } => LossReason::PingTimedOut,
            Self::HolderStopped => LossReason::HolderStopped,
        }
    }
}

/// Why taking the lock failed (as opposed to finding it held elsewhere).
#[derive(Debug, thiserror::Error)]
pub enum TakeError {
    /// Connecting or the lock's statement failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Connecting and taking did not finish within [`TAKE_TIMEOUT`].
    #[error("taking the pipeline lock did not finish within {after:?}")]
    TimedOut { after: Duration },
}

/// The held lock: a task owning the connection. Dropped, it releases.
#[derive(Debug)]
pub struct PipelineLock {
    holder: JoinHandle<()>,
    lost: Option<oneshot::Receiver<LockLoss>>,
}

impl Drop for PipelineLock {
    fn drop(&mut self) {
        self.holder.abort();
    }
}

impl PipelineLock {
    /// Try to take the lock on a new connection to `url`, within
    /// [`TAKE_TIMEOUT`]. `None` when another session holds it.
    ///
    /// Timed out after the server granted the lock, the dropped session
    /// keeps it until the server notices (at most [`SERVER_REAP`] across a
    /// partition); the next try finds it held elsewhere until then.
    pub async fn try_take(url: &DatabaseUrl) -> Result<Option<Self>, TakeError> {
        let connection = tokio::time::timeout(TAKE_TIMEOUT, take(url))
            .await
            .map_err(|_| TakeError::TimedOut {
                after: TAKE_TIMEOUT,
            })??;
        let Some(connection) = connection else {
            return Ok(None);
        };
        tracing::info!(
            ping = ?PING,
            ping_timeout = ?PING_TIMEOUT,
            detection = ?DETECTION,
            "pipeline lock taken"
        );
        let (lost_tx, lost) = oneshot::channel();
        let holder = tokio::spawn(hold(connection, lost_tx));
        Ok(Some(Self {
            holder,
            lost: Some(lost),
        }))
    }

    /// Resolves with why once the lock may be lost (its session failed a
    /// ping or did not answer one in time), at most [`DETECTION`] after
    /// the session went silent. Cancel safe: a call dropped before the
    /// loss leaves it for the next call. Pending forever once it resolved.
    pub async fn lost(&mut self) -> LockLoss {
        let Some(lost) = self.lost.as_mut() else {
            return std::future::pending().await;
        };
        let loss = lost.await.unwrap_or(LockLoss::HolderStopped);
        self.lost = None;
        loss
    }
}

/// Connect and try the lock: the connection when taken, `None` when held
/// elsewhere.
async fn take(url: &DatabaseUrl) -> Result<Option<PgConnection>, StoreError> {
    let options = url
        .connect_options()
        .clone()
        .application_name(&format!("crosstalk-pipeline-lock-{}", std::process::id()));
    let mut connection = PgConnection::connect_with(&options).await?;
    let (taken,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
        .bind(PIPELINE_LOCK)
        .fetch_one(&mut connection)
        .await?;
    if !taken {
        // Closing politely or not, the session never held the lock.
        let _ = tokio::time::timeout(PING_TIMEOUT, connection.close()).await;
        return Ok(None);
    }
    Ok(Some(connection))
}

/// Keep `connection` alive, pinging it every [`PING`] with a deadline of
/// [`PING_TIMEOUT`]; report the first failure or timeout, then end,
/// dropping the connection.
async fn hold(mut connection: PgConnection, lost: oneshot::Sender<LockLoss>) {
    let mut ticker = tokio::time::interval(PING);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let loss = loop {
        ticker.tick().await;
        match tokio::time::timeout(PING_TIMEOUT, connection.ping()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                break LockLoss::PingFailed {
                    failure: crosstalk_store::classify(&error),
                };
            }
            Err(_) => {
                break LockLoss::PingTimedOut {
                    after: PING_TIMEOUT,
                };
            }
        }
    };
    tracing::error!(
        reason = loss.reason(),
        detail = %loss,
        detection = ?DETECTION,
        server_reap = ?SERVER_REAP,
        "pipeline lock session failed; the pipeline incarnation is abandoned"
    );
    let _ = lost.send(loss);
    // Report first, then let go: dropped, not closed, since a polite close
    // would wait on the same dead link.
    drop(connection);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detection_is_one_ping_and_its_deadline() {
        assert_eq!(DETECTION, PING + PING_TIMEOUT);
        assert!(DETECTION <= Duration::from_secs(10), "{DETECTION:?}");
        assert!(DETECTION < SERVER_REAP);
    }

    /// The pipeline task watches `lost` through the start, drops that
    /// call, then watches again while running: the loss must reach the
    /// second call.
    #[tokio::test]
    async fn a_dropped_lost_call_leaves_the_loss_for_the_next() {
        let (lost_tx, lost) = oneshot::channel();
        let mut lock = PipelineLock {
            holder: tokio::spawn(std::future::pending()),
            lost: Some(lost),
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(10), lock.lost())
                .await
                .is_err(),
            "nothing lost yet"
        );
        let loss = LockLoss::PingTimedOut {
            after: PING_TIMEOUT,
        };
        lost_tx.send(loss.clone()).expect("the lock listens");
        let reported = tokio::time::timeout(Duration::from_secs(1), lock.lost())
            .await
            .expect("the loss reaches the next call");
        assert_eq!(reported, loss);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), lock.lost())
                .await
                .is_err(),
            "pending once reported"
        );
    }

    #[test]
    fn each_loss_has_a_distinct_reason() {
        let losses = [
            LockLoss::PingFailed {
                failure: DbFailure::ConnectionLost,
            },
            LockLoss::PingTimedOut {
                after: PING_TIMEOUT,
            },
            LockLoss::HolderStopped,
        ];
        let reasons: std::collections::HashSet<_> = losses.iter().map(LockLoss::reason).collect();
        assert_eq!(reasons.len(), losses.len());
        assert!(
            losses[1].to_string().contains("did not answer a ping"),
            "{}",
            losses[1]
        );
    }
}
