//! The bounds every pipeline connection runs with: `statement_timeout`
//! and `lock_timeout`, set per session as each connection of a pipeline
//! incarnation's pool connects (`gateway::postgres::incarnation_pool_bounded`),
//! from the store section's `pipeline_timeouts`.
//!
//! Across a silent partition the pipeline lock's ping already bounds the
//! wait (the incarnation is abandoned within `store::lock::DETECTION`).
//! These bound the other case: a server that still answers the lock's ping
//! but stalls a pipeline statement (a row or table lock held elsewhere, a
//! stuck vacuum, disk pressure). The server cancels the statement
//! (SQLSTATE `57014` or `55P03`), `crosstalk_store::classify` types it a
//! transient `DbFailure::TimedOut` (counted, logged), and the caller
//! retries: a stage nacks and the delivery is redelivered, the reaper and
//! the checkpoint try again at their next round. Nothing else is bounded
//! by them: capture's publisher pool, the template pool, the `api` role's
//! pool and the lock's session run without (`docs/features/postgres_mode.md`,
//! "Bounded statements").
//!
//! Retention deletes (the bus's log prune, the provenance and ledger
//! expiries) run under a third, much longer bound, `retention`: each raises
//! its transaction's `statement_timeout` to it (`crosstalk_store::
//! raise_for_retention`, through the session settings `apply` records), so
//! a delete grown by a long stop is not cancelled every round, which would
//! silently stop retention. Batching those deletes is the follow-up.
//!
//! Defaults: 20 s per statement, 15 s per lock wait, 15 min per retention
//! delete. The statement bound must stay below the bus's ack timeout
//! (30 s by default; checked at start, [`PipelineTimeouts::check_ack_timeout`]):
//! a stage releases a delivery whose store stalled without spending an
//! attempt, but a hold that times out first is taken back by the reaper
//! and its next take spends one, so a long stall could dead-letter it. Every pipeline
//! statement runs in milliseconds; the longest legitimate ones (retention
//! deletes after a long stop, recovery's reads, a checkpoint's snapshot)
//! stay well inside 20 s on a large deployment, while a stage or the
//! reaper is never parked longer. The lock bound sits above the bus's own
//! tighter ones (a take's 10 s, the reaper's 2 s `SET LOCAL lock_timeout`,
//! which keep theirs) and below the default 30 s ack timeout, so a stage
//! write blocked on a lock is nacked before its hold times out.

use std::time::Duration;

use crosstalk_store::sqlx::{self, PgConnection};
use serde::Deserialize;

/// The pipeline's statement, lock and retention bounds. Valid by
/// construction: each at least a millisecond, within the server's 32-bit
/// millisecond range, the lock bound at most the statement bound (a longer
/// one could never fire: the statement's would first), and the retention
/// bound at least the statement bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "RawPipelineTimeouts")]
pub struct PipelineTimeouts {
    statement_ms: u32,
    lock_ms: u32,
    retention_ms: u32,
}

/// Why a [`PipelineTimeouts`] was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPipelineTimeouts {
    /// Zero (below a millisecond) disables the bound on the server.
    #[error("{setting} must be at least 1 ms (zero disables it)")]
    Zero { setting: &'static str },
    /// The server keeps the setting as a 32-bit count of milliseconds.
    #[error("{setting} of {ms} ms exceeds the server's maximum of 2147483647 ms")]
    TooLarge { setting: &'static str, ms: u64 },
    /// A lock bound above the statement bound never fires.
    #[error("lock_timeout ({lock_ms} ms) must not exceed statement_timeout ({statement_ms} ms)")]
    LockAboveStatement { lock_ms: u64, statement_ms: u64 },
    /// Retention deletes get at least the bound every statement gets.
    #[error(
        "retention_timeout ({retention_ms} ms) must not be below statement_timeout ({statement_ms} ms)"
    )]
    RetentionBelowStatement {
        retention_ms: u64,
        statement_ms: u64,
    },
    /// A statement stalled until its bound must be cancelled before its
    /// delivery's hold times out (`bus.ack_timeout_micros`).
    #[error(
        "statement_timeout ({statement_ms} ms) must be below the bus's ack timeout ({ack_timeout_ms} ms)"
    )]
    StatementNotBelowAckTimeout {
        statement_ms: u64,
        ack_timeout_ms: u64,
    },
}

const STATEMENT: &str = "statement_timeout";
const LOCK: &str = "lock_timeout";
const RETENTION: &str = "retention_timeout";

impl PipelineTimeouts {
    pub const DEFAULT_STATEMENT: Duration = Duration::from_secs(20);
    pub const DEFAULT_LOCK: Duration = Duration::from_secs(15);
    pub const DEFAULT_RETENTION: Duration = Duration::from_secs(15 * 60);

    /// Checked constructor; every bound is kept in whole milliseconds.
    pub fn new(
        statement: Duration,
        lock: Duration,
        retention: Duration,
    ) -> Result<Self, InvalidPipelineTimeouts> {
        let statement_ms = millis(STATEMENT, statement)?;
        let lock_ms = millis(LOCK, lock)?;
        let retention_ms = millis(RETENTION, retention)?;
        if lock_ms > statement_ms {
            return Err(InvalidPipelineTimeouts::LockAboveStatement {
                lock_ms: u64::from(lock_ms),
                statement_ms: u64::from(statement_ms),
            });
        }
        if retention_ms < statement_ms {
            return Err(InvalidPipelineTimeouts::RetentionBelowStatement {
                retention_ms: u64::from(retention_ms),
                statement_ms: u64::from(statement_ms),
            });
        }
        Ok(Self {
            statement_ms,
            lock_ms,
            retention_ms,
        })
    }

    /// How long a statement may run.
    pub fn statement(&self) -> Duration {
        Duration::from_millis(u64::from(self.statement_ms))
    }

    /// How long a statement may wait for a lock.
    pub fn lock(&self) -> Duration {
        Duration::from_millis(u64::from(self.lock_ms))
    }

    /// Refused unless the statement bound is below `ack_timeout`: a stage
    /// whose statement stalls to the bound releases its delivery (no
    /// attempt spent) before the reaper takes the hold back (whose next
    /// take spends one).
    pub fn check_ack_timeout(&self, ack_timeout: Duration) -> Result<(), InvalidPipelineTimeouts> {
        match self.statement() < ack_timeout {
            true => Ok(()),
            false => Err(InvalidPipelineTimeouts::StatementNotBelowAckTimeout {
                statement_ms: u64::from(self.statement_ms),
                ack_timeout_ms: u64::try_from(ack_timeout.as_millis()).unwrap_or(u64::MAX),
            }),
        }
    }

    /// How long a retention delete may run.
    pub fn retention(&self) -> Duration {
        Duration::from_millis(u64::from(self.retention_ms))
    }

    /// The session settings a pipeline connection runs with, as
    /// `set_config` takes them: the server's two bounds, then the two
    /// `crosstalk.*` settings retention deletes raise their transaction's
    /// bound from and restore it to (`crosstalk_store::raise_for_retention`).
    pub fn session_settings(&self) -> [(&'static str, String); 4] {
        let statement = format!("{}ms", self.statement_ms);
        [
            (STATEMENT, statement.clone()),
            (LOCK, format!("{}ms", self.lock_ms)),
            (crosstalk_store::STATEMENT_BOUND, statement),
            (
                crosstalk_store::RETENTION_BOUND,
                format!("{}ms", self.retention_ms),
            ),
        ]
    }

    /// Set the bounds on `connection` for its whole session, in one round
    /// trip.
    pub async fn apply(&self, connection: &mut PgConnection) -> Result<(), sqlx::Error> {
        let [(a, a_value), (b, b_value), (c, c_value), (d, d_value)] = self.session_settings();
        sqlx::query(
            "SELECT set_config($1, $2, false), set_config($3, $4, false), \
             set_config($5, $6, false), set_config($7, $8, false)",
        )
        .bind(a)
        .bind(a_value)
        .bind(b)
        .bind(b_value)
        .bind(c)
        .bind(c_value)
        .bind(d)
        .bind(d_value)
        .execute(connection)
        .await
        .map(|_| ())
    }
}

impl Default for PipelineTimeouts {
    /// 20 s per statement, 15 s per lock wait, 15 min per retention delete
    /// (see the module docs).
    fn default() -> Self {
        Self {
            statement_ms: 20_000,
            lock_ms: 15_000,
            retention_ms: 900_000,
        }
    }
}

fn millis(setting: &'static str, bound: Duration) -> Result<u32, InvalidPipelineTimeouts> {
    let ms = u64::try_from(bound.as_millis()).unwrap_or(u64::MAX);
    if ms == 0 {
        return Err(InvalidPipelineTimeouts::Zero { setting });
    }
    u32::try_from(ms)
        .ok()
        .filter(|ms| i32::try_from(*ms).is_ok())
        .ok_or(InvalidPipelineTimeouts::TooLarge { setting, ms })
}

/// The store section's `pipeline_timeouts`, as written: whole
/// milliseconds, each defaulted.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct RawPipelineTimeouts {
    #[serde(default = "default_statement_ms")]
    statement_ms: u64,
    #[serde(default = "default_lock_ms")]
    lock_ms: u64,
    #[serde(default = "default_retention_ms")]
    retention_ms: u64,
}

fn default_retention_ms() -> u64 {
    u64::try_from(PipelineTimeouts::DEFAULT_RETENTION.as_millis()).unwrap_or(u64::MAX)
}

fn default_statement_ms() -> u64 {
    u64::try_from(PipelineTimeouts::DEFAULT_STATEMENT.as_millis()).unwrap_or(u64::MAX)
}

fn default_lock_ms() -> u64 {
    u64::try_from(PipelineTimeouts::DEFAULT_LOCK.as_millis()).unwrap_or(u64::MAX)
}

impl TryFrom<RawPipelineTimeouts> for PipelineTimeouts {
    type Error = InvalidPipelineTimeouts;

    fn try_from(raw: RawPipelineTimeouts) -> Result<Self, Self::Error> {
        Self::new(
            Duration::from_millis(raw.statement_ms),
            Duration::from_millis(raw.lock_ms),
            Duration::from_millis(raw.retention_ms),
        )
    }
}

#[cfg(test)]
mod tests;
