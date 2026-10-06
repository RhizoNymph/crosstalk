//! L8 surface for crosstalk: the query API, operator actions, the live feed
//! and export, generic over the spec's store traits.
//!
//! Implements [`crosstalk_spec::interfaces::l8_surface`]:
//!
//! - [`Surface`] implements `QueryApi` (module `query`), `OperatorActions`
//!   (module `actions`) and `LiveFeed` ([`live`]) over any [`SurfaceStores`]:
//!   one associated type per spec store trait, read and written only
//!   through the spec's traits.
//! - [`live::FeedWriter`] runs the live feed's log; [`live::FeedHandle`]
//!   feeds it, ends sessions and opens streams.
//! - [`export`] plans, audits and streams exports; [`export::SpecExportSource`]
//!   is an `ExportSource` over the spec's read traits.
//! - [`nodes::NodeCache`] is the spec's `NodeFacts`, kept current from
//!   L3's and L5's events by [`nodes::NodeFeeder`]; the wiring hands it to
//!   the edge store, whose graphs describe their nodes with it.
//! - Time comes from the injected spec `Clock`; ids the surface mints come
//!   from one ULID generator over it.
//! - [`pg`] holds L8's Postgres stores: `PgAuditLog` (`AuditLog` and
//!   `AuditIntents`), `PgOperatorStore` and `PgSinkRegistry`, in schema
//!   `surface`. Operator actions record a write-ahead intent before their
//!   effect, and [`Surface::recover_interrupted`] turns those a stopped
//!   process left into `Interrupted` entries at start.
//! - Cursor keys: [`Surface::with_secret`] derives the surface's from the
//!   deployment secret, so its cursors survive a restart; [`Surface::new`]
//!   draws one (tests, memory mode).
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
pub mod nodes;
pub mod pg;
mod query;
mod service;
pub mod stores;

pub use config::SurfaceConfig;
pub use cursor::SURFACE_CURSOR_LABEL;
pub use nodes::{NodeCache, NodeFeeder};
pub use service::Surface;
pub use stores::{EvidenceRecords, RecordReadError, SurfaceStores};

#[cfg(test)]
mod dst;
#[cfg(test)]
mod integration;
#[cfg(test)]
mod props;
#[cfg(test)]
mod tests;
