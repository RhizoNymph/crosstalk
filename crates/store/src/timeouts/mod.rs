//! Statements the server cancelled at a bound: `statement_timeout`
//! (SQLSTATE `57014`, `query_canceled`) or `lock_timeout` (`55P03`,
//! `lock_not_available`).
//!
//! The gateway runs every connection of a pipeline incarnation with both
//! bounds set, so a server that answers but stalls a statement (a row or
//! table lock held elsewhere, a stuck vacuum, disk pressure) cancels it
//! rather than hanging a stage or the bus's reaper. [`classify`](crate::classify)
//! types the cancel as [`DbFailure::TimedOut`](crate::DbFailure::TimedOut),
//! which is transient (the caller's own retry, a nack and redelivery or the
//! next poll, can succeed) but not retried inside a serializable
//! transaction (that would multiply the stall).
//!
//! Every timeout [`classify`](crate::classify) sees is counted here by
//! kind, process-wide, and logged once at warn with its SQLSTATE and the
//! server's message; the caller's own log line names the statement or
//! stage. [`statement_timeouts`] reads the counts (`/metrics`:
//! `crosstalk_pg_statement_timeouts_total{kind}`).

use std::sync::atomic::{AtomicU64, Ordering};

/// Which bound cancelled a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimeoutKind {
    /// `statement_timeout`: the statement ran past its bound (SQLSTATE
    /// `57014`, which a cancel request also raises).
    Statement,
    /// `lock_timeout`: the statement waited for a lock past its bound
    /// (SQLSTATE `55P03`).
    Lock,
}

impl TimeoutKind {
    /// Both kinds, in label order of the metric.
    pub const ALL: [TimeoutKind; 2] = [TimeoutKind::Statement, TimeoutKind::Lock];

    /// The SQLSTATE the server reports.
    pub const fn sqlstate(self) -> &'static str {
        match self {
            TimeoutKind::Statement => "57014",
            TimeoutKind::Lock => "55P03",
        }
    }

    /// The metric's `kind` label.
    pub const fn label(self) -> &'static str {
        match self {
            TimeoutKind::Statement => "statement",
            TimeoutKind::Lock => "lock",
        }
    }

    /// The kind a SQLSTATE names, if it is a timeout's.
    pub(crate) fn of_sqlstate(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.sqlstate() == code)
    }
}

/// How many statements the server cancelled at each bound since the
/// process started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeoutCounts {
    /// Cancelled by `statement_timeout`.
    pub statement: u64,
    /// Cancelled by `lock_timeout`.
    pub lock: u64,
}

impl TimeoutCounts {
    /// The count of `kind`.
    pub fn of(self, kind: TimeoutKind) -> u64 {
        match kind {
            TimeoutKind::Statement => self.statement,
            TimeoutKind::Lock => self.lock,
        }
    }
}

static STATEMENT: AtomicU64 = AtomicU64::new(0);
static LOCK: AtomicU64 = AtomicU64::new(0);

/// The process's counts so far.
pub fn statement_timeouts() -> TimeoutCounts {
    TimeoutCounts {
        statement: STATEMENT.load(Ordering::Relaxed),
        lock: LOCK.load(Ordering::Relaxed),
    }
}

/// Count a cancel of `kind` and log it (warn: kind, SQLSTATE, the server's
/// message). Called by [`classify`](crate::classify), once per failed
/// statement it is handed.
pub(crate) fn note(kind: TimeoutKind, message: &str) {
    let counter = match kind {
        TimeoutKind::Statement => &STATEMENT,
        TimeoutKind::Lock => &LOCK,
    };
    let total = counter.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    tracing::warn!(
        kind = kind.label(),
        sqlstate = kind.sqlstate(),
        message,
        total,
        "postgres cancelled a statement at its bound; the caller retries it"
    );
}

#[cfg(test)]
mod tests;
