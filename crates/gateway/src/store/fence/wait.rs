//! A new holder's wait for the fence: try [`PIPELINE_FENCE`] exclusively
//! every [`FENCE_POLL`] on the lock's own session until it is granted (no
//! connection of an earlier incarnation is left), then release it.
//!
//! The wait is a loop of short statements, never a blocking
//! `pg_advisory_lock`: dropping it (a stop) cancels nothing on the server,
//! and each try, bounded by the lock's [`PING_TIMEOUT`], is the lock
//! session's liveness check while it waits. A try that fails or does not
//! answer is a lock loss like any other ([`LockLoss`]). Otherwise the wait
//! never gives up: safety over liveness. Who still holds the fence (their
//! `application_name`s, from `pg_locks` and `pg_stat_activity`) is logged
//! when the wait starts and then every [`FENCE_REPORT`] at most.

use std::future::Future;
use std::time::Duration;

use crosstalk_store::sqlx;
use crosstalk_store::sqlx::postgres::PgConnection;
use tokio::time::Instant;

use super::{Epoch, FENCE_POLL, FENCE_REPORT, PIPELINE_FENCE};
use crate::store::lock::{LockLoss, PING_TIMEOUT};

/// A session holding [`PIPELINE_FENCE`], as the waiting holder sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocker {
    pub pid: i32,
    pub application_name: String,
}

/// What the wait runs its statements on: the lock's session.
pub trait FenceSession: Send {
    /// `pg_try_advisory_lock(PIPELINE_FENCE)`: granted or not.
    fn try_exclusive(&mut self) -> impl Future<Output = Result<bool, sqlx::Error>> + Send;
    /// `pg_advisory_unlock(PIPELINE_FENCE)`: whether it was held.
    fn release_exclusive(&mut self) -> impl Future<Output = Result<bool, sqlx::Error>> + Send;
    /// The other sessions holding the fence.
    fn blockers(&mut self) -> impl Future<Output = Result<Vec<Blocker>, sqlx::Error>> + Send;
}

impl FenceSession for PgConnection {
    async fn try_exclusive(&mut self) -> Result<bool, sqlx::Error> {
        let (granted,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(PIPELINE_FENCE)
            .fetch_one(&mut *self)
            .await?;
        Ok(granted)
    }

    async fn release_exclusive(&mut self) -> Result<bool, sqlx::Error> {
        let (held,): (bool,) = sqlx::query_as("SELECT pg_advisory_unlock($1)")
            .bind(PIPELINE_FENCE)
            .fetch_one(&mut *self)
            .await?;
        Ok(held)
    }

    async fn blockers(&mut self) -> Result<Vec<Blocker>, sqlx::Error> {
        let rows: Vec<(i32, String)> = sqlx::query_as(
            "SELECT l.pid, coalesce(a.application_name, '') \
             FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
             WHERE l.locktype = 'advisory' AND l.granted AND l.pid <> pg_backend_pid() \
               AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
               AND l.classid::bigint = $1 >> 32 AND l.objid::bigint = $1 & 4294967295 \
               AND l.objsubid = 1 \
             ORDER BY l.pid",
        )
        .bind(PIPELINE_FENCE)
        .fetch_all(&mut *self)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(pid, application_name)| Blocker {
                pid,
                application_name,
            })
            .collect())
    }
}

/// How a completed wait went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fenced {
    /// From the first try to the grant.
    pub waited: Duration,
    /// Exclusive tries, the granted one included.
    pub polls: u64,
    /// Times the blockers were read and logged.
    pub reports: u64,
}

/// Why the wait ended without the fence.
#[derive(Debug, thiserror::Error)]
pub enum FenceWaitError {
    /// The lock's session failed or went silent while waiting: the lock
    /// may be gone.
    #[error(transparent)]
    Lost(#[from] LockLoss),
    /// Releasing the exclusive fence found it not held: the session lost
    /// it without failing a statement (it should never happen).
    #[error("the exclusive pipeline fence was not held at its release")]
    NotHeld,
}

/// Run one statement of the wait on the lock's session within the lock's
/// [`PING_TIMEOUT`]; a failure or no answer is a lock loss.
async fn bounded<T>(
    statement: impl Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, LockLoss> {
    match tokio::time::timeout(PING_TIMEOUT, statement).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(LockLoss::PingFailed {
            failure: crosstalk_store::classify(&error),
        }),
        Err(_) => Err(LockLoss::PingTimedOut {
            after: PING_TIMEOUT,
        }),
    }
}

/// Wait until the fence is free and take it exclusively, then release it:
/// see the module docs. `epoch` is the one the take bumped to (for the
/// logs).
pub async fn wait_for_fence<S: FenceSession>(
    session: &mut S,
    epoch: Epoch,
) -> Result<Fenced, FenceWaitError> {
    let started = Instant::now();
    let mut polls = 0_u64;
    let mut reports = 0_u64;
    let mut last_report: Option<Instant> = None;
    loop {
        polls += 1;
        if bounded(session.try_exclusive()).await? {
            break;
        }
        if last_report.is_none_or(|at| at.elapsed() >= FENCE_REPORT) {
            let blockers = bounded(session.blockers()).await?;
            last_report = Some(Instant::now());
            reports += 1;
            let names: Vec<String> = blockers
                .iter()
                .map(|blocker| format!("{} (pid {})", blocker.application_name, blocker.pid))
                .collect();
            tracing::warn!(
                epoch = epoch.get(),
                waited_ms = millis(started.elapsed()),
                blocking_sessions = blockers.len(),
                blockers = ?names,
                "pipeline fencing: waiting for the previous owner's connections to close"
            );
        }
        tokio::time::sleep(FENCE_POLL).await;
    }
    let waited = started.elapsed();
    if !bounded(session.release_exclusive()).await? {
        return Err(FenceWaitError::NotHeld);
    }
    if reports > 0 {
        tracing::info!(
            epoch = epoch.get(),
            waited_ms = millis(waited),
            polls,
            "pipeline fencing: the previous owner's connections are gone"
        );
    }
    Ok(Fenced {
        waited,
        polls,
        reports,
    })
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
