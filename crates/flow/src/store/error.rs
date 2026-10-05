//! The flow stores' errors.
//!
//! The spec's L5 errors carry a store failure as `Store { reason }`. Inside
//! a transaction body a failure is a [`Fault`]: a database error stays a
//! [`TxError::Db`], so the serializable retry sees serialization failures
//! and deadlocks, and anything else (a stored value that does not decode or
//! breaks a spec rule) aborts the transaction as the operation's own
//! `Store` error ([`StoreFault`]).
//!
//! [`FlowStoreError`] is the typed error of what this module adds beyond the
//! spec traits: connecting, migrating, the directory refresh, the outbox
//! relay and the shard checkpoints.

use crosstalk_spec::interfaces::l5_flow::channels::TrafficError;
use crosstalk_spec::interfaces::l5_flow::transmissions::TransmissionStoreError;
use crosstalk_spec::interfaces::l5_flow::verdicts::VerdictError;
use crosstalk_spec::interfaces::l5_flow::{PromoteError, RegistryError};
use crosstalk_store::{DbFailure, SerializableError, StoreError, TxError, classify};

use super::codec::CodecError;
use super::ids::IdSourceError;

/// What failed in a flow store operation, below the spec error it reports.
#[derive(Debug, thiserror::Error)]
pub enum FlowStoreError {
    /// The store harness failed: a migration, a connection, or a
    /// serializable transaction that ran out of retries.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A statement failed.
    #[error("query failed: {failure}")]
    Query {
        /// The classified cause.
        failure: DbFailure,
        /// The driver error.
        #[source]
        source: sqlx::Error,
    },
    /// A stored value did not convert.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Stored rows break a rule the spec types check (a policy history out
    /// of order, a verdict log with a gap).
    #[error("stored {what} is invalid: {reason}")]
    Corrupt {
        /// What was read.
        what: &'static str,
        /// The spec's refusal.
        reason: String,
    },
    /// No channel id could be drawn.
    #[error(transparent)]
    Ids(#[from] IdSourceError),
}

impl From<sqlx::Error> for FlowStoreError {
    fn from(source: sqlx::Error) -> Self {
        FlowStoreError::Query {
            failure: classify(&source),
            source,
        }
    }
}

/// A failure inside a transaction body.
#[derive(Debug)]
pub(crate) enum Fault {
    /// A statement failed: retried when it is a serialization failure or a
    /// deadlock.
    Db(sqlx::Error),
    /// Anything else, which aborts the transaction.
    Other(FlowStoreError),
}

impl From<sqlx::Error> for Fault {
    fn from(error: sqlx::Error) -> Self {
        Fault::Db(error)
    }
}

impl From<CodecError> for Fault {
    fn from(error: CodecError) -> Self {
        Fault::Other(error.into())
    }
}

impl From<IdSourceError> for Fault {
    fn from(error: IdSourceError) -> Self {
        Fault::Other(error.into())
    }
}

impl Fault {
    /// Stored rows broke a spec rule.
    pub(crate) fn corrupt(what: &'static str, reason: impl std::fmt::Debug) -> Self {
        Fault::Other(FlowStoreError::Corrupt {
            what,
            reason: format!("{reason:?}"),
        })
    }

    /// Outside a transaction: the typed error.
    pub(crate) fn into_error(self) -> FlowStoreError {
        match self {
            Fault::Db(error) => error.into(),
            Fault::Other(error) => error,
        }
    }
}

/// A spec error with a `Store { reason }` variant.
pub(crate) trait StoreFault: Sized {
    /// The store failure `error`, as this spec error.
    fn store(error: FlowStoreError) -> Self;
}

macro_rules! store_fault {
    ($($error:ty),* $(,)?) => {$(
        impl StoreFault for $error {
            fn store(error: FlowStoreError) -> Self {
                Self::Store { reason: error.to_string() }
            }
        }
    )*};
}

store_fault!(
    RegistryError,
    TrafficError,
    PromoteError,
    TransmissionStoreError,
    VerdictError
);

impl<E: StoreFault> From<Fault> for TxError<E> {
    fn from(fault: Fault) -> Self {
        match fault {
            Fault::Db(error) => TxError::Db(error),
            Fault::Other(error) => TxError::Abort(E::store(error)),
        }
    }
}

/// The spec error a finished serializable transaction failed with.
pub(crate) fn finished<E: StoreFault>(error: SerializableError<E>) -> E {
    match error {
        SerializableError::Aborted(error) => error,
        SerializableError::Store(error) => E::store(error.into()),
    }
}

/// The spec error of a failed statement outside a serializable
/// transaction (reads).
pub(crate) fn failed<E: StoreFault>(fault: impl Into<Fault>) -> E {
    E::store(fault.into().into_error())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crosstalk_spec::ids::ChannelId;

    #[test]
    fn a_database_fault_stays_retryable_and_others_abort() {
        let db: TxError<RegistryError> = Fault::Db(sqlx::Error::PoolTimedOut).into();
        assert!(matches!(db, TxError::Db(sqlx::Error::PoolTimedOut)));
        let other: TxError<RegistryError> = Fault::corrupt("policy history", "out of order").into();
        assert!(matches!(
            other,
            TxError::Abort(RegistryError::Store { reason }) if reason.contains("policy history")
        ));
    }

    #[test]
    fn an_abort_keeps_the_operations_own_error() {
        let id = ChannelId::from_ulid(7);
        let error = finished(SerializableError::Aborted(TrafficError::UnknownChannel(id)));
        assert_eq!(error, TrafficError::UnknownChannel(id));
    }
}
