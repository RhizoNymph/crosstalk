//! The store's internal failure, and how it maps into the spec's errors.

use crosstalk_spec::interfaces::l7_topology::{EdgeError, EdgeQueryError};
use crosstalk_store::{DbFailure, classify};

use crate::codec::CodecError;

/// Why a database round trip failed. Each spec error it becomes is a
/// `Store { reason }`: the classified failure and the message, never a
/// bound value.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("{failure:?}: {source}")]
    Sql {
        failure: DbFailure,
        #[source]
        source: sqlx::Error,
    },
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// What the store read breaks a rule it keeps (a bucket that sums to
    /// nothing, a share out of range): a bug or a hand-edited table.
    #[error("{0}")]
    Inconsistent(String),
}

impl From<sqlx::Error> for DbError {
    fn from(source: sqlx::Error) -> Self {
        Self::Sql {
            failure: classify(&source),
            source,
        }
    }
}

impl DbError {
    pub fn inconsistent(reason: impl Into<String>) -> Self {
        Self::Inconsistent(reason.into())
    }

    /// The classified failure, for a database error.
    pub fn failure(&self) -> Option<&DbFailure> {
        match self {
            Self::Sql { failure, .. } => Some(failure),
            Self::Codec(_) | Self::Inconsistent(_) => None,
        }
    }
}

/// A store method's failure: a refusal the spec names (`E`), or a
/// database failure, which becomes `E`'s `Store { reason }`.
#[derive(Debug)]
pub enum Failed<E> {
    Refused(E),
    Db(DbError),
}

impl<E> From<DbError> for Failed<E> {
    fn from(error: DbError) -> Self {
        Self::Db(error)
    }
}

impl<E> From<sqlx::Error> for Failed<E> {
    fn from(error: sqlx::Error) -> Self {
        Self::Db(error.into())
    }
}

impl<E> From<CodecError> for Failed<E> {
    fn from(error: CodecError) -> Self {
        Self::Db(error.into())
    }
}

impl From<EdgeError> for Failed<EdgeError> {
    fn from(error: EdgeError) -> Self {
        Self::Refused(error)
    }
}

impl From<EdgeQueryError> for Failed<EdgeQueryError> {
    fn from(error: EdgeQueryError) -> Self {
        Self::Refused(error)
    }
}

impl From<Failed<EdgeError>> for EdgeError {
    fn from(failed: Failed<EdgeError>) -> Self {
        match failed {
            Failed::Refused(error) => error,
            Failed::Db(error) => Self::Store {
                reason: error.to_string(),
            },
        }
    }
}

impl From<Failed<EdgeQueryError>> for EdgeQueryError {
    fn from(failed: Failed<EdgeQueryError>) -> Self {
        match failed {
            Failed::Refused(error) => error,
            Failed::Db(error) => Self::Store {
                reason: error.to_string(),
            },
        }
    }
}

/// A write's result inside the store.
pub type WriteResult<T> = Result<T, Failed<EdgeError>>;

/// A read's result inside the store.
pub type ReadResult<T> = Result<T, Failed<EdgeQueryError>>;
