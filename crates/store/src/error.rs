//! Typed store errors and the classification of driver errors.
//!
//! [`classify`] turns a [`sqlx::Error`] into a [`DbFailure`]: the handful of
//! outcomes a layer crate maps into its spec error (a unique violation is a
//! conflict, a serialization failure is retried, a lost connection is
//! unavailability). Everything else is `Server` (with its SQLSTATE) or
//! `Client` (a driver-side failure such as a decode error).

use std::fmt;
use std::num::NonZeroU32;

use crate::config::ConfigError;
use crate::extensions::Extension;
use crate::layer::Layer;

/// What a failed database call means to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbFailure {
    /// SQLSTATE `23505`: a unique or primary-key constraint refused the row.
    UniqueViolation {
        /// The constraint, when the server named it.
        constraint: Option<String>,
    },
    /// SQLSTATE `23503`: a foreign key refused the row.
    ForeignKeyViolation {
        /// The constraint, when the server named it.
        constraint: Option<String>,
    },
    /// SQLSTATE `23514`: a check constraint refused the row.
    CheckViolation {
        /// The constraint, when the server named it.
        constraint: Option<String>,
    },
    /// SQLSTATE `40001`: a serializable transaction lost a conflict. Retry
    /// the whole transaction.
    SerializationFailure,
    /// SQLSTATE `40P01`: the transaction was chosen as a deadlock victim.
    /// Retry the whole transaction.
    DeadlockDetected,
    /// The connection is gone: an I/O or TLS error, a closed pool, a crashed
    /// connection worker, SQLSTATE class `08` (connection exception) or
    /// `57P01`–`57P03` (the server is shutting down or not yet up). Whether
    /// an in-flight commit landed is unknown.
    ConnectionLost,
    /// No pooled connection became free within the acquire timeout.
    PoolTimedOut,
    /// A query expected a row and got none.
    RowNotFound,
    /// Any other error the server reported, with its SQLSTATE.
    Server {
        /// The five-character SQLSTATE, when the error carried one.
        sqlstate: Option<String>,
    },
    /// A driver-side failure: configuration, encoding, decoding, protocol,
    /// a missing column or type.
    Client,
}

impl DbFailure {
    /// Whether re-running the whole transaction can succeed: a serialization
    /// failure or a deadlock.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            DbFailure::SerializationFailure | DbFailure::DeadlockDetected
        )
    }

    /// Whether the failure is about reaching the database rather than the
    /// statement: a lost connection or a pool timeout. A layer reports these
    /// as unavailability.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, DbFailure::ConnectionLost | DbFailure::PoolTimedOut)
    }
}

impl fmt::Display for DbFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let named = |f: &mut fmt::Formatter<'_>, what: &str, c: &Option<String>| match c {
            Some(c) => write!(f, "{what} ({c})"),
            None => f.write_str(what),
        };
        match self {
            DbFailure::UniqueViolation { constraint } => named(f, "unique violation", constraint),
            DbFailure::ForeignKeyViolation { constraint } => {
                named(f, "foreign key violation", constraint)
            }
            DbFailure::CheckViolation { constraint } => named(f, "check violation", constraint),
            DbFailure::SerializationFailure => f.write_str("serialization failure"),
            DbFailure::DeadlockDetected => f.write_str("deadlock detected"),
            DbFailure::ConnectionLost => f.write_str("connection lost"),
            DbFailure::PoolTimedOut => f.write_str("pool timed out"),
            DbFailure::RowNotFound => f.write_str("row not found"),
            DbFailure::Server {
                sqlstate: Some(code),
            } => write!(f, "server error (SQLSTATE {code})"),
            DbFailure::Server { sqlstate: None } => f.write_str("server error"),
            DbFailure::Client => f.write_str("driver error"),
        }
    }
}

/// SQLSTATEs [`classify`] gives a meaning to.
mod sqlstate {
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const CHECK_VIOLATION: &str = "23514";
    pub const SERIALIZATION_FAILURE: &str = "40001";
    pub const DEADLOCK_DETECTED: &str = "40P01";
    /// Class 08, connection exception.
    pub const CONNECTION_EXCEPTION_CLASS: &str = "08";
    pub const ADMIN_SHUTDOWN: &str = "57P01";
    pub const CRASH_SHUTDOWN: &str = "57P02";
    pub const CANNOT_CONNECT_NOW: &str = "57P03";
}

/// Classifies a driver error.
pub fn classify(err: &sqlx::Error) -> DbFailure {
    match err {
        sqlx::Error::Database(db) => classify_database(db.as_ref()),
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => DbFailure::ConnectionLost,
        sqlx::Error::PoolTimedOut => DbFailure::PoolTimedOut,
        sqlx::Error::RowNotFound => DbFailure::RowNotFound,
        sqlx::Error::Migrate(m) => match m.as_ref() {
            sqlx::migrate::MigrateError::Execute(inner)
            | sqlx::migrate::MigrateError::ExecuteMigration(inner, _) => classify(inner),
            _ => DbFailure::Client,
        },
        _ => DbFailure::Client,
    }
}

fn classify_database(db: &dyn sqlx::error::DatabaseError) -> DbFailure {
    let constraint = || db.constraint().map(str::to_owned);
    let Some(code) = db.code() else {
        return DbFailure::Server { sqlstate: None };
    };
    match code.as_ref() {
        sqlstate::UNIQUE_VIOLATION => DbFailure::UniqueViolation {
            constraint: constraint(),
        },
        sqlstate::FOREIGN_KEY_VIOLATION => DbFailure::ForeignKeyViolation {
            constraint: constraint(),
        },
        sqlstate::CHECK_VIOLATION => DbFailure::CheckViolation {
            constraint: constraint(),
        },
        sqlstate::SERIALIZATION_FAILURE => DbFailure::SerializationFailure,
        sqlstate::DEADLOCK_DETECTED => DbFailure::DeadlockDetected,
        sqlstate::ADMIN_SHUTDOWN | sqlstate::CRASH_SHUTDOWN | sqlstate::CANNOT_CONNECT_NOW => {
            DbFailure::ConnectionLost
        }
        c if c.starts_with(sqlstate::CONNECTION_EXCEPTION_CLASS) => DbFailure::ConnectionLost,
        c => DbFailure::Server {
            sqlstate: Some(c.to_owned()),
        },
    }
}

/// Why a required extension is not usable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtensionProblem {
    /// The server has no such extension installed
    /// (`pg_available_extensions` does not list it).
    NotAvailable,
    /// The extension is available but `CREATE EXTENSION` failed, typically
    /// for lack of privilege.
    CreateFailed(DbFailure),
}

impl fmt::Display for ExtensionProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtensionProblem::NotAvailable => f.write_str("not available on the server"),
            ExtensionProblem::CreateFailed(failure) => write!(f, "could not be created: {failure}"),
        }
    }
}

/// Every way the store fails.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The configuration was refused.
    #[error("store configuration: {0}")]
    Config(#[from] ConfigError),
    /// The pool could not open its first connection.
    #[error("connecting to postgres at {target}: {failure}")]
    Connect {
        /// `host:port/database`; never the user or password.
        target: String,
        /// The classified cause.
        failure: DbFailure,
        /// The driver error.
        #[source]
        source: sqlx::Error,
    },
    /// A layer's migrations failed (a bad migration, a checksum mismatch
    /// against an applied one, a dirty version, or the SQL itself).
    #[error("migrating layer {layer}: {source}")]
    Migrate {
        /// The layer whose migrations failed.
        layer: Layer,
        /// The migrator's error.
        #[source]
        source: sqlx::migrate::MigrateError,
    },
    /// A required extension is not usable.
    #[error("required extension {extension} is missing: {problem}")]
    ExtensionMissing {
        /// The extension.
        extension: Extension,
        /// Why.
        problem: ExtensionProblem,
    },
    /// A statement failed.
    #[error("query failed: {failure}")]
    Query {
        /// The classified cause.
        failure: DbFailure,
        /// The driver error.
        #[source]
        source: sqlx::Error,
    },
    /// A serializable transaction kept conflicting until its retry budget
    /// ran out.
    #[error("serializable transaction gave up after {attempts} attempts: {failure}")]
    RetriesExhausted {
        /// Attempts made, the first included.
        attempts: NonZeroU32,
        /// The last attempt's classified failure (retryable by definition).
        failure: DbFailure,
        /// The last attempt's driver error.
        #[source]
        source: sqlx::Error,
    },
}

impl StoreError {
    /// The classified database failure behind this error, when there is one.
    pub fn failure(&self) -> Option<&DbFailure> {
        match self {
            StoreError::Connect { failure, .. }
            | StoreError::Query { failure, .. }
            | StoreError::RetriesExhausted { failure, .. } => Some(failure),
            StoreError::ExtensionMissing {
                problem: ExtensionProblem::CreateFailed(failure),
                ..
            } => Some(failure),
            StoreError::Config(_)
            | StoreError::Migrate { .. }
            | StoreError::ExtensionMissing { .. } => None,
        }
    }
}

impl From<sqlx::Error> for StoreError {
    /// A failed statement, classified.
    fn from(source: sqlx::Error) -> Self {
        StoreError::Query {
            failure: classify(&source),
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::error::Error as StdError;

    /// A server error as the driver reports it, without a server.
    #[derive(Debug)]
    struct FakeDbError {
        code: Option<&'static str>,
        constraint: Option<&'static str>,
    }

    impl fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "fake database error {:?}", self.code)
        }
    }

    impl StdError for FakeDbError {}

    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            "fake"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.map(Cow::Borrowed)
        }
        fn constraint(&self) -> Option<&str> {
            self.constraint
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

    fn db(code: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError {
            code: Some(code),
            constraint: None,
        }))
    }

    fn db_on(code: &'static str, constraint: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError {
            code: Some(code),
            constraint: Some(constraint),
        }))
    }

    #[test]
    fn unique_violation_carries_its_constraint() {
        assert_eq!(
            classify(&db_on("23505", "agents_pkey")),
            DbFailure::UniqueViolation {
                constraint: Some("agents_pkey".to_owned())
            }
        );
        assert_eq!(
            classify(&db("23505")),
            DbFailure::UniqueViolation { constraint: None }
        );
    }

    #[test]
    fn integrity_violations_are_typed() {
        assert_eq!(
            classify(&db_on("23503", "edges_agent_fk")),
            DbFailure::ForeignKeyViolation {
                constraint: Some("edges_agent_fk".to_owned())
            }
        );
        assert_eq!(
            classify(&db_on("23514", "weight_positive")),
            DbFailure::CheckViolation {
                constraint: Some("weight_positive".to_owned())
            }
        );
    }

    #[test]
    fn serialization_failure_and_deadlock_are_retryable() {
        let ser = classify(&db("40001"));
        assert_eq!(ser, DbFailure::SerializationFailure);
        assert!(ser.is_retryable());
        let dead = classify(&db("40P01"));
        assert_eq!(dead, DbFailure::DeadlockDetected);
        assert!(dead.is_retryable());
    }

    #[test]
    fn only_conflicts_are_retryable() {
        let not = [
            DbFailure::UniqueViolation { constraint: None },
            DbFailure::ForeignKeyViolation { constraint: None },
            DbFailure::CheckViolation { constraint: None },
            DbFailure::ConnectionLost,
            DbFailure::PoolTimedOut,
            DbFailure::RowNotFound,
            DbFailure::Server {
                sqlstate: Some("42P01".to_owned()),
            },
            DbFailure::Client,
        ];
        for f in not {
            assert!(!f.is_retryable(), "{f:?}");
        }
    }

    #[test]
    fn connection_loss_is_typed() {
        for code in [
            "08000", "08003", "08006", "08P01", "57P01", "57P02", "57P03",
        ] {
            assert_eq!(classify(&db(code)), DbFailure::ConnectionLost, "{code}");
        }
        let io = sqlx::Error::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert_eq!(classify(&io), DbFailure::ConnectionLost);
        assert_eq!(
            classify(&sqlx::Error::PoolClosed),
            DbFailure::ConnectionLost
        );
        assert_eq!(
            classify(&sqlx::Error::WorkerCrashed),
            DbFailure::ConnectionLost
        );
        assert!(DbFailure::ConnectionLost.is_unavailable());
    }

    #[test]
    fn pool_timeout_and_missing_row_are_typed() {
        let timed_out = classify(&sqlx::Error::PoolTimedOut);
        assert_eq!(timed_out, DbFailure::PoolTimedOut);
        assert!(timed_out.is_unavailable());
        assert_eq!(classify(&sqlx::Error::RowNotFound), DbFailure::RowNotFound);
        assert!(!DbFailure::RowNotFound.is_unavailable());
    }

    #[test]
    fn other_server_errors_keep_their_sqlstate() {
        assert_eq!(
            classify(&db("42P01")),
            DbFailure::Server {
                sqlstate: Some("42P01".to_owned())
            }
        );
        let no_code = sqlx::Error::Database(Box::new(FakeDbError {
            code: None,
            constraint: None,
        }));
        assert_eq!(classify(&no_code), DbFailure::Server { sqlstate: None });
    }

    #[test]
    fn driver_side_errors_are_client_failures() {
        assert_eq!(
            classify(&sqlx::Error::Protocol("bad frame".to_owned())),
            DbFailure::Client
        );
        assert_eq!(
            classify(&sqlx::Error::ColumnNotFound("id".to_owned())),
            DbFailure::Client
        );
    }

    #[test]
    fn migration_execute_errors_classify_their_cause() {
        let wrapped =
            sqlx::Error::Migrate(Box::new(sqlx::migrate::MigrateError::Execute(db("40001"))));
        assert_eq!(classify(&wrapped), DbFailure::SerializationFailure);
        let in_migration = sqlx::Error::Migrate(Box::new(
            sqlx::migrate::MigrateError::ExecuteMigration(db("42P01"), 4),
        ));
        assert_eq!(
            classify(&in_migration),
            DbFailure::Server {
                sqlstate: Some("42P01".to_owned())
            }
        );
        let dirty = sqlx::Error::Migrate(Box::new(sqlx::migrate::MigrateError::Dirty(3)));
        assert_eq!(classify(&dirty), DbFailure::Client);
    }

    #[test]
    fn store_error_from_sqlx_is_a_classified_query_error() {
        let err = StoreError::from(db_on("23505", "k"));
        assert!(matches!(
            &err,
            StoreError::Query {
                failure: DbFailure::UniqueViolation { constraint: Some(c) },
                ..
            } if c == "k"
        ));
        assert_eq!(
            err.failure(),
            Some(&DbFailure::UniqueViolation {
                constraint: Some("k".to_owned())
            })
        );
        assert!(err.source().is_some());
    }

    #[test]
    fn store_error_failure_covers_extension_creation() {
        let err = StoreError::ExtensionMissing {
            extension: Extension::Vector,
            problem: ExtensionProblem::CreateFailed(DbFailure::Server {
                sqlstate: Some("42501".to_owned()),
            }),
        };
        assert!(err.failure().is_some());
        let missing = StoreError::ExtensionMissing {
            extension: Extension::PgTrgm,
            problem: ExtensionProblem::NotAvailable,
        };
        assert_eq!(missing.failure(), None);
        assert_eq!(
            missing.to_string(),
            "required extension pg_trgm is missing: not available on the server"
        );
    }

    #[test]
    fn failures_display_readably() {
        assert_eq!(
            DbFailure::UniqueViolation {
                constraint: Some("k".to_owned())
            }
            .to_string(),
            "unique violation (k)"
        );
        assert_eq!(
            DbFailure::Server {
                sqlstate: Some("42P01".to_owned())
            }
            .to_string(),
            "server error (SQLSTATE 42P01)"
        );
    }
}
