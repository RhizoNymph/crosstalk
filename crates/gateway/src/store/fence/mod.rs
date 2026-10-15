//! The pipeline fence: no write from an older pipeline incarnation once a
//! newer one runs (`transport.pipeline-lock.no-write-from-an-older-epoch`;
//! `docs/features/postgres_mode.md`, "The fence").
//!
//! The pipeline lock alone does not exclude two writers: when the lock's
//! session is ended while the database stays reachable
//! (`pg_terminate_backend`, a backend crash), the old holder learns of it
//! only at its next ping, up to [`super::lock::PING`] later, and its
//! incarnation's pool still has live connections meanwhile. Another
//! process may take the lock at once. The fence closes that window with two
//! things in the database:
//!
//! - the **epoch** (`transport.pipeline_epoch`, one row), which every new
//!   holder of the lock bumps on its lock session, committed, as soon as it
//!   has the lock; and
//! - a second advisory key, [`PIPELINE_FENCE`], which every connection of a
//!   pipeline incarnation holds **shared** for its whole life ([`join`],
//!   run in the incarnation pool's `after_connect`), and which a new holder
//!   takes **exclusively** after the bump, waiting until every shared
//!   holder is gone ([`wait::wait_for_fence`]), then releases at once.
//!
//! ```text
//! new holder (lock session):  try_lock(LOCK) ─▶ UPDATE epoch + 1 (commit) ─▶ try_lock(FENCE) until granted ─▶ unlock(FENCE)
//! pipeline connection:        connect ─▶ session setup ─▶ try_lock_shared(FENCE) ─▶ read epoch == mine? else refuse
//! ```
//!
//! **Why the order makes it safe.** The new holder commits the bump before
//! it first tries the exclusive fence, and a connection takes its shared
//! fence before it reads the epoch. Take any connection of an older
//! incarnation. Either it took its shared fence before the new holder's
//! exclusive was granted: the exclusive cannot be granted while it holds
//! it, so the new holder waits for the connection to close. Or it took it
//! after the exclusive was released: then it reads the epoch after the
//! bump committed, sees a newer one than its incarnation's, and refuses
//! itself. A shared try while the exclusive is held is refused outright.
//! So once the new holder's take returns, no connection of an older epoch
//! exists and none can be made, in any process: the new incarnation starts
//! with no older writer left.
//!
//! The lock's session, capture's publisher pool, the process's template
//! pool and the `api` role's pool never join the fence: capture is never
//! fenced (INV-1237), and none of them writes what only the lock's holder
//! writes.
//!
//! Safety over liveness: the wait never gives up. An old owner that is
//! stopped (`SIGSTOP`) with its connections open blocks the takeover until
//! its connections die (the server's keepalives, or the process resuming,
//! noticing its loss and closing them).

use std::fmt;
use std::time::Duration;

use crosstalk_store::sqlx;
use crosstalk_store::sqlx::postgres::PgConnection;
use serde::{Deserialize, Serialize};

pub use super::lock::PIPELINE_FENCE;

pub mod wait;

#[cfg(test)]
mod tests;

/// How often a new holder tries the exclusive fence while earlier
/// connections hold it. Each try is also the lock session's liveness check
/// while it waits.
pub const FENCE_POLL: Duration = Duration::from_millis(250);

/// How often, at most, a waiting holder reads and logs who still holds the
/// fence.
pub const FENCE_REPORT: Duration = Duration::from_secs(5);

/// How many times the pipeline lock has been taken (`transport.pipeline_epoch`);
/// each holder runs its incarnation under the epoch its take bumped to.
/// 0 before the first take.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Epoch(u64);

impl Epoch {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The epoch the next take bumps to.
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A stored epoch that is not one (the table's check forbids it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EpochError {
    #[error("the stored pipeline epoch {stored} is negative")]
    Negative { stored: i64 },
}

impl TryFrom<i64> for Epoch {
    type Error = EpochError;

    fn try_from(stored: i64) -> Result<Self, Self::Error> {
        u64::try_from(stored)
            .map(Self)
            .map_err(|_| EpochError::Negative { stored })
    }
}

/// Why a connection may not serve a pipeline incarnation.
#[derive(Debug, thiserror::Error)]
pub enum FenceError {
    /// A newer holder took the lock: the incarnation of `mine` is over.
    #[error("the pipeline incarnation of epoch {mine} is fenced: the current epoch is {current}")]
    Fenced { mine: Epoch, current: Epoch },
    /// A new holder holds the fence exclusively (it is fencing).
    #[error(
        "the pipeline incarnation of epoch {mine} is fenced: a new holder is fencing (epoch {current})"
    )]
    Fencing { mine: Epoch, current: Epoch },
    #[error(transparent)]
    Epoch(#[from] EpochError),
    #[error("joining the pipeline fence: {0}")]
    Database(#[from] sqlx::Error),
}

impl FenceError {
    /// As `after_connect` must return it: a statement's own error as it
    /// is, a refusal as an I/O error that keeps this one (the connection
    /// cannot be used; sqlx closes it and logs the error).
    pub fn into_sqlx(self) -> sqlx::Error {
        match self {
            Self::Database(error) => error,
            refused => sqlx::Error::Io(std::io::Error::other(refused)),
        }
    }
}

/// Read the stored epoch.
pub async fn read_epoch(connection: &mut PgConnection) -> Result<Epoch, FenceError> {
    let (stored,): (i64,) = sqlx::query_as("SELECT epoch FROM transport.pipeline_epoch")
        .fetch_one(&mut *connection)
        .await?;
    Ok(Epoch::try_from(stored)?)
}

/// Join the fence as a connection of the incarnation of `mine`: take
/// [`PIPELINE_FENCE`] shared (held until the connection closes), then
/// read the epoch. Refused when a new holder holds the fence exclusively
/// or the epoch is no longer `mine`; the caller then drops the connection
/// (sqlx closes it), which releases a shared fence it took. The order
/// (shared fence, then epoch) is half of the fence's safety argument (the
/// module docs).
pub async fn join(connection: &mut PgConnection, mine: Epoch) -> Result<(), FenceError> {
    let (shared,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock_shared($1)")
        .bind(PIPELINE_FENCE)
        .fetch_one(&mut *connection)
        .await?;
    let current = read_epoch(connection).await?;
    match (shared, current == mine) {
        (true, true) => Ok(()),
        (false, _) => Err(FenceError::Fencing { mine, current }),
        (true, false) => Err(FenceError::Fenced { mine, current }),
    }
}

/// Bump the epoch on the lock's session, autocommit: the epoch this take
/// runs its incarnation under. Committed before the fence is first tried
/// (the other half of the safety argument).
pub(crate) async fn bump(connection: &mut PgConnection) -> Result<Epoch, FenceError> {
    let (stored,): (i64,) = sqlx::query_as(
        "UPDATE transport.pipeline_epoch SET epoch = epoch + 1 WHERE singleton RETURNING epoch",
    )
    .fetch_one(&mut *connection)
    .await?;
    Ok(Epoch::try_from(stored)?)
}
