//! What L6's Postgres stores share: the schema's migrations, the column
//! codec, page cursors, the outbox they publish from, and the storage
//! failure every store maps into its spec error.
//!
//! Every table lives in schema `analysis` (`crosstalk_store::Layer::Analysis`)
//! and every runtime query names it, so no query depends on `search_path`.

pub mod codec;
pub mod cursor;
pub mod outbox;
#[cfg(test)]
pub(crate) mod testing;

use crosstalk_spec::ids::mint::UlidExhausted;
use crosstalk_store::{Layer, Migrations, SerializableError, StoreError, migrate};
use sqlx::PgPool;

pub use codec::CodecError;
pub use cursor::CursorKey;
pub use outbox::{BusSink, ChannelSink, EventSink, OutboxError, SinkError};

/// L6's embedded migrations (`crates/analysis/migrations`), run in schema
/// `analysis`.
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run L6's migrations on `pool`. The `vector` extension must exist
/// (`crosstalk_store::ensure_extensions`).
pub async fn run_migrations(pool: &PgPool) -> Result<(), StoreError> {
    migrate(pool, Layer::Analysis, Migrations::Embedded(&MIGRATIONS)).await
}

/// Why a Postgres store call failed for a reason of its own rather than a
/// refusal the spec names. Each store maps it into its spec error's
/// `Store { reason }`.
#[derive(Debug, thiserror::Error)]
pub enum StorageFailure {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("query failed: {0}")]
    Query(#[from] sqlx::Error),
    #[error("stored data: {0}")]
    Codec(#[from] CodecError),
    #[error("outbox: {0}")]
    Outbox(#[from] OutboxError),
    #[error("no id is left to mint: {0}")]
    Ids(UlidExhausted),
    #[error("the revision counter of {0} is exhausted")]
    RevisionExhausted(String),
    #[error("{0}")]
    Invariant(String),
}

impl StorageFailure {
    /// The text a spec error's `Store { reason }` carries.
    pub fn reason(&self) -> String {
        self.to_string()
    }
}

impl From<SerializableError<StorageFailure>> for StorageFailure {
    fn from(error: SerializableError<StorageFailure>) -> Self {
        match error {
            SerializableError::Aborted(failure) => failure,
            SerializableError::Store(store) => Self::Store(store),
        }
    }
}
