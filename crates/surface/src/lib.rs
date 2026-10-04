//! L8 surface for crosstalk: the query API, operator actions, the live feed
//! and export, generic over the spec's store traits.
//!
//! Implements [`crosstalk_spec::interfaces::l8_surface`]:
//!
//! - [`Surface`] implements `QueryApi` ([`query`]), `OperatorActions`
//!   ([`actions`]) and `LiveFeed` ([`live`]) over any [`SurfaceStores`]:
//!   one associated type per spec store trait, read and written only
//!   through the spec's traits.
//! - [`live::FeedWriter`] runs the live feed's log; [`live::FeedHandle`]
//!   feeds it, ends sessions and opens streams.
//! - [`export`] plans, audits and streams exports; [`export::SpecExportSource`]
//!   is an `ExportSource` over the spec's read traits.
//! - Time comes from the injected spec `Clock`; ids the surface mints come
//!   from one ULID generator over it.
//!
//! Roadmap: P2.6 (L8 surface over spec traits) and P7.3 (surface on Postgres).
//! A layer crate: it depends on the spec, never on another layer crate.

mod actions;
mod audit;
pub mod config;
mod cursor;
pub mod export;
mod ids;
pub mod live;
mod query;
mod service;
pub mod stores;

pub use config::SurfaceConfig;
pub use service::Surface;
pub use stores::{EvidenceRecords, RecordReadError, SurfaceStores};

#[cfg(test)]
mod tests;
