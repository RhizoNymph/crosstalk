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
//! Taking the lock is bounded too ([`TAKE_TIMEOUT`]): a connect,
//! `pg_try_advisory_lock` or the epoch's bump across a partition fails as
//! [`TakeError::TimedOut`] instead of hanging the pipeline's start.
//!
//! # The fence
//!
//! Once it has the lock, a take bumps the pipeline epoch and waits until
//! no connection of an earlier pipeline incarnation (of any process) is
//! left, on the same session ([`super::fence`]): the lock is handed out
//! only then, with its [`Epoch`] ([`PipelineLock::epoch`]). The wait does
//! not count against [`TAKE_TIMEOUT`] and never gives up; a lock session
//! that fails while waiting is a [`TakeError::Lost`].
//!
//! # Releasing on a planned stop
//!
//! A planned handover ends with [`PipelineLock::release`]: the holder
//! unlocks (`pg_advisory_unlock`) on its own session and closes it, so a
//! standby's next try finds the lock free without waiting for the session
//! to end. It takes the handover's [`PoolsClosed`] proof, which only the
//! handover's last step can make: the lock cannot be released before the
//! incarnation's checkpoints are persisted and its pool is closed
//! (`gateway::postgres::handover`). A lock dropped instead is released
//! when its session ends.

use std::time::Duration;

use crosstalk_store::sqlx::postgres::PgConnection;
use crosstalk_store::sqlx::{self, Connection};
use crosstalk_store::{DatabaseUrl, DbFailure, StoreError};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::fence::wait::{FenceWaitError, Fenced, wait_for_fence};
use super::fence::{self, Epoch, FenceError};
use crate::gateway::postgres::handover::PoolsClosed;
use crate::live::recovery::LossReason;

/// The advisory lock key (`crosstal` in ASCII).
pub const PIPELINE_LOCK: i64 = 0x6372_6f73_7374_616c;

/// The pipeline fence's advisory key (`xtalkfnc` in ASCII), distinct from
/// [`PIPELINE_LOCK`]: every connection of a pipeline incarnation holds it
/// shared; a new holder of the lock takes it exclusively to wait until no
/// connection of an earlier incarnation is left ([`super::fence`]).
pub const PIPELINE_FENCE: i64 = 0x7874_616c_6b66_6e63;

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

/// How long taking the lock (connect, `pg_try_advisory_lock` and the
/// epoch's bump) may take. The fence wait after it is not counted.
pub const TAKE_TIMEOUT: Duration = Duration::from_secs(10);

// INV-1236: the holder notices a lost session strictly before the server
// can hand the lock to another process.
const _: () = assert!(DETECTION.as_millis() < SERVER_REAP.as_millis());
const _: () = assert!(PING_TIMEOUT.as_millis() < PING.as_millis());
// The fence has a key of its own.
const _: () = assert!(PIPELINE_FENCE != PIPELINE_LOCK);

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
    /// Bumping the epoch failed.
    #[error("bumping the pipeline epoch: {0}")]
    Epoch(#[source] FenceError),
    /// Connecting, taking and the bump did not finish within
    /// [`TAKE_TIMEOUT`].
    #[error("taking the pipeline lock did not finish within {after:?}")]
    TimedOut { after: Duration },
    /// The lock's session failed or went silent while waiting for the
    /// fence: the lock may be gone, a loss like any other.
    #[error("the pipeline lock was lost while fencing: {0}")]
    Lost(LockLoss),
    /// The exclusive fence was gone at its release.
    #[error("the exclusive pipeline fence was not held at its release")]
    FenceNotHeld,
}

impl From<FenceWaitError> for TakeError {
    fn from(error: FenceWaitError) -> Self {
        match error {
            FenceWaitError::Lost(loss) => Self::Lost(loss),
            FenceWaitError::NotHeld => Self::FenceNotHeld,
        }
    }
}

/// Why [`PipelineLock::release`] did not unlock explicitly. The session
/// is dropped either way, and the server releases the lock as it ends it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReleaseError {
    /// The lock's session had already failed (its loss was reported, or
    /// its holder stopped): nothing left to unlock.
    #[error("the lock's session was already gone: {0}")]
    Lost(LockLoss),
    /// `pg_advisory_unlock` answered that the session did not hold it.
    #[error("pg_advisory_unlock found the pipeline lock not held by its session")]
    NotHeld,
    /// The unlock failed.
    #[error("unlocking the pipeline lock failed: {failure}")]
    Failed { failure: DbFailure },
    /// The unlock did not answer within [`PING_TIMEOUT`].
    #[error("unlocking the pipeline lock did not answer within {after:?}")]
    TimedOut { after: Duration },
}

/// A request to the holder task: unlock, close, answer.
type ReleaseRequest = oneshot::Sender<Result<(), ReleaseError>>;

/// The held lock: a task owning the connection, and the epoch its take
/// bumped to. Dropped, it releases (the session dropped); released with
/// [`PipelineLock::release`], it unlocks first.
#[derive(Debug)]
pub struct PipelineLock {
    holder: JoinHandle<()>,
    lost: Option<oneshot::Receiver<LockLoss>>,
    release: Option<oneshot::Sender<ReleaseRequest>>,
    epoch: Epoch,
}

impl Drop for PipelineLock {
    fn drop(&mut self) {
        self.holder.abort();
    }
}

impl PipelineLock {
    /// Try to take the lock on a new connection to `url`, then fence the
    /// earlier incarnations: [`PipelineLock::try_take_reporting`] reporting
    /// nothing.
    pub async fn try_take(url: &DatabaseUrl) -> Result<Option<Self>, TakeError> {
        Self::try_take_reporting(url, |_| {}).await
    }

    /// Try to take the lock on a new connection to `url` and bump the
    /// epoch, within [`TAKE_TIMEOUT`]; `None` when another session holds
    /// the lock. Then call `fencing` with the new epoch and wait, however
    /// long it takes, until no connection of an earlier incarnation holds
    /// the fence ([`super::fence`]). Cancel safe: dropped at any point,
    /// the session is dropped and the lock with it (the bump stays, which
    /// only fences more).
    ///
    /// Timed out after the server granted the lock, the dropped session
    /// keeps it until the server notices (at most [`SERVER_REAP`] across a
    /// partition); the next try finds it held elsewhere until then. A
    /// session that fails while fencing is [`TakeError::Lost`].
    pub async fn try_take_reporting(
        url: &DatabaseUrl,
        fencing: impl FnOnce(Epoch) + Send,
    ) -> Result<Option<Self>, TakeError> {
        let taken = tokio::time::timeout(TAKE_TIMEOUT, take(url))
            .await
            .map_err(|_| TakeError::TimedOut {
                after: TAKE_TIMEOUT,
            })??;
        let Some((mut connection, epoch)) = taken else {
            return Ok(None);
        };
        fencing(epoch);
        let Fenced {
            waited,
            polls,
            reports,
        } = wait_for_fence(&mut connection, epoch).await?;
        tracing::info!(
            epoch = epoch.get(),
            fence_waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
            fence_polls = polls,
            fence_reports = reports,
            ping = ?PING,
            ping_timeout = ?PING_TIMEOUT,
            detection = ?DETECTION,
            "pipeline lock taken; earlier incarnations fenced"
        );
        let (lost_tx, lost) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let holder = tokio::spawn(hold(connection, lost_tx, released));
        Ok(Some(Self {
            holder,
            lost: Some(lost),
            release: Some(release),
            epoch,
        }))
    }

    /// Release the lock explicitly, the last step of a planned handover:
    /// the holder unlocks on its session (`pg_advisory_unlock`, within
    /// [`PING_TIMEOUT`]) and closes it, so the next taker finds it free at
    /// its next try. Only a handover whose incarnation's pool is closed
    /// can call it ([`PoolsClosed`]). Not unlocked (the session already
    /// lost, the unlock refused or not answering): the session is dropped
    /// and the server releases the lock as it ends it. Cancel safe: dropped
    /// before its answer, the lock is dropped, and with it the session.
    pub async fn release(mut self, _closed: PoolsClosed) -> Result<(), ReleaseError> {
        let Some(release) = self.release.take() else {
            return Err(ReleaseError::Lost(LockLoss::HolderStopped));
        };
        let (reply, answer) = oneshot::channel();
        if release.send(reply).is_err() {
            return Err(ReleaseError::Lost(self.reported_loss()));
        }
        match answer.await {
            Ok(released) => released,
            Err(_) => Err(ReleaseError::Lost(self.reported_loss())),
        }
    }

    /// The loss the holder reported, if it did; else that it stopped.
    fn reported_loss(&mut self) -> LockLoss {
        self.lost
            .as_mut()
            .and_then(|lost| lost.try_recv().ok())
            .unwrap_or(LockLoss::HolderStopped)
    }

    /// The epoch this taking of the lock bumped to: the one its
    /// incarnation runs under.
    pub fn epoch(&self) -> Epoch {
        self.epoch
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

/// Connect, try the lock and, taken, bump the epoch: the connection and
/// the new epoch, `None` when held elsewhere.
async fn take(url: &DatabaseUrl) -> Result<Option<(PgConnection, Epoch)>, TakeError> {
    let options = url
        .connect_options()
        .clone()
        .application_name(&format!("crosstalk-pipeline-lock-{}", std::process::id()));
    let mut connection = PgConnection::connect_with(&options)
        .await
        .map_err(StoreError::from)?;
    let (taken,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
        .bind(PIPELINE_LOCK)
        .fetch_one(&mut connection)
        .await
        .map_err(StoreError::from)?;
    if !taken {
        // Closing politely or not, the session never held the lock.
        let _ = tokio::time::timeout(PING_TIMEOUT, connection.close()).await;
        return Ok(None);
    }
    // Committed (autocommit) before the fence is first tried.
    let epoch = fence::bump(&mut connection)
        .await
        .map_err(TakeError::Epoch)?;
    Ok(Some((connection, epoch)))
}

/// Keep `connection` alive, pinging it every [`PING`] with a deadline of
/// [`PING_TIMEOUT`]; report the first failure or timeout, then end,
/// dropping the connection. Asked to release between two pings: unlock,
/// close, answer and end.
async fn hold(
    mut connection: PgConnection,
    lost: oneshot::Sender<LockLoss>,
    mut release: oneshot::Receiver<ReleaseRequest>,
) {
    let mut ticker = tokio::time::interval(PING);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let loss = loop {
        tokio::select! {
            biased;
            request = &mut release => {
                // The lock dropped without a release (its task is being
                // aborted): the session goes with this task.
                let Ok(reply) = request else { return };
                let _ = reply.send(unlock(connection).await);
                return;
            }
            _ = ticker.tick() => {}
        }
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

/// Unlock on the lock's own session, then close it politely; a session
/// whose unlock failed or did not answer is dropped instead (a close would
/// wait on the same link), which ends it and the lock with it.
async fn unlock(mut connection: PgConnection) -> Result<(), ReleaseError> {
    let unlocked = tokio::time::timeout(
        PING_TIMEOUT,
        sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
            .bind(PIPELINE_LOCK)
            .fetch_one(&mut connection),
    )
    .await;
    let released = match unlocked {
        Ok(Ok(true)) => Ok(()),
        Ok(Ok(false)) => Err(ReleaseError::NotHeld),
        Ok(Err(error)) => Err(ReleaseError::Failed {
            failure: crosstalk_store::classify(&error),
        }),
        Err(_) => Err(ReleaseError::TimedOut {
            after: PING_TIMEOUT,
        }),
    };
    match &released {
        Ok(()) => {
            if tokio::time::timeout(PING_TIMEOUT, connection.close())
                .await
                .is_err()
            {
                tracing::warn!("the pipeline lock's session did not close in time; dropped");
            }
        }
        Err(_) => drop(connection),
    }
    released
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
            release: None,
            epoch: Epoch::new(1),
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
