//! How a storage failure becomes a spec error's `Store { reason }`, inside a
//! `retry_serializable` body and outside one. Every L6 Postgres store maps
//! its failures through these.

use crosstalk_store::{SerializableError, TxError};

use super::StorageFailure;

/// A spec error with a `Store { reason }` variant, which every storage
/// failure becomes.
pub trait Failure: Sized {
    fn store(reason: String) -> Self;
}

/// Implement [`Failure`] for spec errors whose `Store` variant is
/// `Store { reason: String }`.
macro_rules! failure {
    ($($error:ty),* $(,)?) => {
        $(
            impl $crate::pg::tx::Failure for $error {
                fn store(reason: String) -> Self {
                    Self::Store { reason }
                }
            }
        )*
    };
}

pub(crate) use failure;

/// A storage failure inside a transaction body. A driver error goes back to
/// `retry_serializable` (which retries a serialization failure or deadlock);
/// anything else aborts with the spec error's `Store` variant.
pub(crate) fn abort<E: Failure>(failure: impl Into<StorageFailure>) -> TxError<E> {
    failure.into().into_tx(|failure| E::store(failure.reason()))
}

/// A storage failure outside a transaction.
pub(crate) fn fail<E: Failure>(failure: impl Into<StorageFailure>) -> E {
    E::store(failure.into().reason())
}

/// A finished transaction's result as the spec error.
pub(crate) fn finish<T, E: Failure>(result: Result<T, SerializableError<E>>) -> Result<T, E> {
    result.map_err(|error| match error {
        SerializableError::Aborted(error) => error,
        SerializableError::Store(store) => E::store(store.to_string()),
    })
}
