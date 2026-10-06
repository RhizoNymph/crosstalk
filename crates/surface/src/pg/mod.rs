//! L8's Postgres stores (schema `surface`, `crosstalk_store::Layer::Surface`):
//!
//! - [`PgAuditLog`]: `AuditLog` and `AuditIntents`. Append-only (a trigger
//!   refuses every `UPDATE` and `DELETE` of the log); write-ahead intents
//!   in `action_intents`, completed with their entry in one transaction and
//!   recovered as `Interrupted` at start (`surface.audit.no-silent-effect`).
//!   Page cursors are keyed by a key derived from the deployment secret, so
//!   they resolve after a restart (`surface.cursor.survives-restart`).
//! - [`PgOperatorStore`]: `OperatorStore`. A load stores the new directory
//!   and appends its config entries to the same log in one transaction
//!   (`surface.audit.config-changes-recorded`); the audit ids it mints come
//!   from a generator the composer seeds from OS entropy
//!   (`surface.ids.unique-across-restart`).
//! - [`PgSinkRegistry`]: `SinkRegistry`, the configured sinks and their
//!   last deliveries.
//!
//! ```text
//! act ─▶ intend ─▶ INSERT action_intents ───────────────────────────────┐
//!        (effect in L3/L5/L6) ─▶ complete ─▶ txn: INSERT audit + subjects, DELETE action_intents
//! start ─▶ recover_interrupted ─▶ per intent, oldest first: txn: INSERT Interrupted entry, DELETE intent
//! config load ─▶ txn: read directory ─ OperatorDirectory::load ─▶ UPSERT directory + INSERT config entries
//! ```
//!
//! Every write is one `SERIALIZABLE` transaction under
//! `crosstalk_store::retry_serializable`; every query names the schema, so
//! none depends on `search_path`. No store reads a clock: times come in as
//! arguments or inside the values stored.

mod audit;
mod codec;
mod operators;
mod sinks;
#[cfg(test)]
pub(crate) mod testing;

use crosstalk_store::{Layer, Migrations, SerializableError, StoreError, migrate};
use sqlx::PgPool;

pub use audit::{AUDIT_CURSOR_LABEL, PgAuditLog};
pub use codec::CodecError;
pub use operators::PgOperatorStore;
pub use sinks::{PgSinkRegistry, SinkConfig};

/// L8's embedded migrations (`crates/surface/migrations`), run in schema
/// `surface`.
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run L8's migrations on `pool`. No extension is needed.
pub async fn run_migrations(pool: &PgPool) -> Result<(), StoreError> {
    migrate(pool, Layer::Surface, Migrations::Embedded(&MIGRATIONS)).await
}

/// Why a Postgres store call failed for a reason of its own rather than a
/// refusal the spec names. Each store maps it into its spec error's
/// `Store { reason }`.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StorageFailure {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("query failed: {0}")]
    Query(#[from] sqlx::Error),
    #[error("stored data: {0}")]
    Codec(#[from] CodecError),
}

impl StorageFailure {
    /// The text a spec error's `Store { reason }` carries.
    pub(crate) fn reason(&self) -> String {
        self.to_string()
    }
}

/// The error a transaction body aborted with, or the store failure that
/// ended the retries, as the body's own error type.
pub(crate) fn settle<E>(error: SerializableError<E>, store: impl FnOnce(StorageFailure) -> E) -> E {
    match error {
        SerializableError::Aborted(refused) => refused,
        SerializableError::Store(failure) => store(StorageFailure::Store(failure)),
    }
}
