//! A server-side cancel at a bound: typed, transient, never retried inside
//! a serializable transaction, and counted by kind.

use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;

use super::*;
use crate::error::{DbFailure, classify};

/// A server error as the driver reports it, without a server.
#[derive(Debug)]
struct ServerError(&'static str);

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "canceling statement (SQLSTATE {})", self.0)
    }
}

impl StdError for ServerError {}

impl sqlx::error::DatabaseError for ServerError {
    fn message(&self) -> &str {
        "canceling statement due to a timeout"
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.0))
    }
    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> sqlx::error::ErrorKind {
        sqlx::error::ErrorKind::Other
    }
}

fn server(code: &'static str) -> sqlx::Error {
    sqlx::Error::Database(Box::new(ServerError(code)))
}

/// `57014` (`query_canceled`: `statement_timeout`) and `55P03`
/// (`lock_not_available`: `lock_timeout`) are typed by kind.
#[test]
fn statement_and_lock_timeouts_are_typed_by_kind() {
    assert_eq!(
        classify(&server("57014")),
        DbFailure::TimedOut(TimeoutKind::Statement)
    );
    assert_eq!(
        classify(&server("55P03")),
        DbFailure::TimedOut(TimeoutKind::Lock)
    );
    assert_eq!(TimeoutKind::Statement.sqlstate(), "57014");
    assert_eq!(TimeoutKind::Lock.sqlstate(), "55P03");
    assert_eq!(TimeoutKind::Statement.label(), "statement");
    assert_eq!(TimeoutKind::Lock.label(), "lock");
}

/// A timeout is transient (a later try can succeed) but neither a lost
/// connection nor a conflict: `retry_serializable` does not re-run the
/// transaction on it (that would multiply the stall), the caller's own
/// retry (a nack and redelivery, the next poll) does.
#[test]
fn a_timeout_is_transient_and_not_retried_in_place() {
    for kind in [TimeoutKind::Statement, TimeoutKind::Lock] {
        let failure = DbFailure::TimedOut(kind);
        assert!(failure.is_timeout(), "{failure:?}");
        assert!(failure.is_transient(), "{failure:?}");
        assert!(!failure.is_retryable(), "{failure:?}");
        assert!(!failure.is_unavailable(), "{failure:?}");
    }
}

/// What else is transient: a lost connection, a pool timeout, a conflict.
/// A refused statement is not.
#[test]
fn transient_failures_are_reaching_the_database_conflicts_and_timeouts() {
    for failure in [
        DbFailure::ConnectionLost,
        DbFailure::PoolTimedOut,
        DbFailure::SerializationFailure,
        DbFailure::DeadlockDetected,
        DbFailure::TimedOut(TimeoutKind::Statement),
        DbFailure::TimedOut(TimeoutKind::Lock),
    ] {
        assert!(failure.is_transient(), "{failure:?}");
    }
    for failure in [
        DbFailure::UniqueViolation { constraint: None },
        DbFailure::ForeignKeyViolation { constraint: None },
        DbFailure::CheckViolation { constraint: None },
        DbFailure::RowNotFound,
        DbFailure::Server {
            sqlstate: Some("42P01".to_owned()),
        },
        DbFailure::Client,
    ] {
        assert!(!failure.is_transient(), "{failure:?}");
        assert!(!failure.is_timeout(), "{failure:?}");
    }
}

/// The display names the bound and its SQLSTATE, which every caller's log
/// line carries.
#[test]
fn a_timeout_displays_its_kind_and_sqlstate() {
    assert_eq!(
        DbFailure::TimedOut(TimeoutKind::Statement).to_string(),
        "statement timeout (SQLSTATE 57014)"
    );
    assert_eq!(
        DbFailure::TimedOut(TimeoutKind::Lock).to_string(),
        "lock timeout (SQLSTATE 55P03)"
    );
}

/// Classifying a timeout counts it under its kind (process-wide: other
/// tests may count too, so only the increase is checked).
#[test]
fn classifying_a_timeout_counts_it_by_kind() {
    let before = statement_timeouts();
    let _ = classify(&server("57014"));
    let _ = classify(&server("55P03"));
    let _ = classify(&server("55P03"));
    let after = statement_timeouts();
    assert!(
        after.statement > before.statement,
        "{before:?} -> {after:?}"
    );
    assert!(after.lock >= before.lock + 2, "{before:?} -> {after:?}");
    assert_eq!(after.of(TimeoutKind::Statement), after.statement);
    assert_eq!(after.of(TimeoutKind::Lock), after.lock);
}

/// Other failures are not counted as timeouts.
#[test]
fn other_failures_are_not_counted() {
    // Counted only on a timeout's SQLSTATE: classifying others changes
    // neither count by itself. Other tests may count concurrently, so the
    // check is that this test's classifications are not timeouts.
    for code in ["40001", "40P01", "23505", "42P01", "57P01"] {
        assert!(!classify(&server(code)).is_timeout(), "{code}");
    }
}
