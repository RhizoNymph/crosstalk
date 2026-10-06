//! How storage failures become the spec's L3 errors.
//!
//! Every L3 spec error has one storage variant, `Store { reason }`. A
//! failure below the spec (a database error, a stored value the spec
//! refuses, ids running out) is classified first ([`StorageFailure`]) and
//! then converted, once, through [`StoreReason`].

use crosstalk_spec::ids::mint::UlidExhausted;
use crosstalk_spec::interfaces::l3_reconstruction::agents::AgentReadError;
use crosstalk_spec::interfaces::l3_reconstruction::conversations::ConversationReadError;
use crosstalk_spec::interfaces::l3_reconstruction::lifecycle::AgentLifecycleError;
use crosstalk_spec::interfaces::l3_reconstruction::{ResolveError, ThreadError};
use crosstalk_store::{DbFailure, SerializableError, StoreError, classify};

use crate::agents::codec::CodecError;

/// A failure below the spec.
#[derive(Debug, thiserror::Error)]
pub enum StorageFailure {
    #[error("database {failure}")]
    Db {
        failure: DbFailure,
        #[source]
        source: sqlx::Error,
    },
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error("no id is left to mint: {0}")]
    Ids(UlidExhausted),
    #[error("the message body {hash} is not in the blob store")]
    MissingBody { hash: String },
    #[error("the blob store failed: {reason}")]
    Blobs { reason: String },
    #[error("stored conversations disagree: {reason}")]
    Inconsistent { reason: String },
}

impl From<sqlx::Error> for StorageFailure {
    fn from(source: sqlx::Error) -> Self {
        Self::Db {
            failure: classify(&source),
            source,
        }
    }
}

/// A spec error's storage variant.
pub trait StoreReason: Sized {
    fn store(reason: String) -> Self;

    /// The spec error for `failure`.
    fn from_failure(failure: &StorageFailure) -> Self {
        Self::store(failure.to_string())
    }

    /// The spec error for a failed serializable transaction: the body's
    /// own refusal as it was, anything else as a storage failure.
    fn from_tx(error: SerializableError<Self>) -> Self {
        match error {
            SerializableError::Aborted(refusal) => refusal,
            SerializableError::Store(store) => Self::from_failure(&StorageFailure::Store(store)),
        }
    }
}

impl StoreReason for ResolveError {
    fn store(reason: String) -> Self {
        Self::Store { reason }
    }
}

impl StoreReason for AgentLifecycleError {
    fn store(reason: String) -> Self {
        Self::Store { reason }
    }
}

impl StoreReason for AgentReadError {
    fn store(reason: String) -> Self {
        Self::Store { reason }
    }
}

impl StoreReason for ConversationReadError {
    fn store(reason: String) -> Self {
        Self::Store { reason }
    }
}

impl StoreReason for ThreadError {
    fn store(reason: String) -> Self {
        Self::Store { reason }
    }
}

/// A failure inside a transaction body: a database error, which the retry
/// loop classifies (and retries when it is a serialization failure), or
/// anything else, which aborts the transaction.
#[derive(Debug)]
pub(crate) enum TxFailure {
    Db(sqlx::Error),
    Other(StorageFailure),
}

impl From<sqlx::Error> for TxFailure {
    fn from(error: sqlx::Error) -> Self {
        Self::Db(error)
    }
}

impl From<CodecError> for TxFailure {
    fn from(error: CodecError) -> Self {
        Self::Other(StorageFailure::Codec(error))
    }
}

impl From<StorageFailure> for TxFailure {
    fn from(error: StorageFailure) -> Self {
        Self::Other(error)
    }
}

impl From<TxFailure> for StorageFailure {
    fn from(failure: TxFailure) -> Self {
        match failure {
            TxFailure::Db(error) => error.into(),
            TxFailure::Other(failure) => failure,
        }
    }
}

/// `failure` as a transaction body's error: a database error stays one,
/// so it is classified and retried; anything else aborts as `E`'s storage
/// variant.
pub(crate) fn tx<E: StoreReason>(failure: TxFailure) -> crosstalk_store::TxError<E> {
    match failure {
        TxFailure::Db(error) => crosstalk_store::TxError::Db(error),
        TxFailure::Other(failure) => crosstalk_store::TxError::Abort(E::from_failure(&failure)),
    }
}
