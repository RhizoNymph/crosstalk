//! The L5 stores on Postgres (roadmap P5): the channel registry and the
//! transmission store, with the flow layer's migrations.
//!
//! - [`PgChannelRegistry`] implements `ChannelRegistry`, `ChannelTraffic`,
//!   `ChannelReads` and `ChannelDirectory`: declarations, policy decisions
//!   and their history, promotion and supersession, resources (on a channel
//!   or on none), accesses, discovery, the recorded state of every channel
//!   transmission and the reads built from them.
//! - [`PgTransmissionStore`] implements `TransmissionStore` and
//!   `TransmissionVerdicts` over the stored transmissions and their verdict
//!   logs.
//! - [`PgExtractionLedger`] implements the extraction step's ledger
//!   (`crate::extract::step::ExtractionLedger`).
//! - [`PgShardTicks`] keeps the correlator shards' tick checkpoints, and
//!   [`ShardKey`] is the shard key the registry's directory decides.
//!
//! **Publishing.** A store publishes what it decides (`ChannelDiscovered`,
//! `ChannelPromoted`, `VerdictSet`, `Changed`) through a transactional
//! outbox: staged in the deciding transaction, relayed to the
//! [`EventSink`] after commit ([`outbox`]).
//!
//! **Transactions.** Every write is one `SERIALIZABLE` transaction under
//! `crosstalk_store::retry_serializable`; every read one `REPEATABLE READ`
//! snapshot. Time is always an argument.
//!
//! The tables live in schema `flow` ([`crosstalk_store::Layer::Flow`]);
//! [`migrate`] runs `crates/flow/migrations/`. See
//! `docs/features/flow_store.md`.

mod codec;
mod cursor;
mod directory;
mod error;
mod ids;
mod ledger;
pub mod outbox;
mod registry;
mod shards;
mod transmissions;

#[cfg(test)]
mod tests;

pub use codec::CodecError;
pub use cursor::prune_cursors;
pub use directory::{ShardIndex, ShardKey};
pub use error::FlowStoreError;
pub use ids::{ChannelIdSource, IdSourceError, UlidChannelIds};
pub use ledger::PgExtractionLedger;
pub use outbox::{EventSink, Relay};
pub use registry::PgChannelRegistry;
pub use shards::PgShardTicks;
pub use transmissions::PgTransmissionStore;

use crosstalk_store::{Layer, Migrations, Store};

/// The flow layer's migrations, embedded at compile time.
pub static MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Run the flow layer's migrations in schema `flow`.
pub async fn migrate(store: &Store) -> Result<(), FlowStoreError> {
    store
        .migrate(Layer::Flow, Migrations::Embedded(&MIGRATIONS))
        .await?;
    Ok(())
}
