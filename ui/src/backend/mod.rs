//! The data source behind every page.
//!
//! Pages and data routes read through the spec's L8 traits,
//! `crosstalk_spec::interfaces::l8_surface::{QueryApi, OperatorActions}`,
//! plus the two gaps in `crate::contract` (`present::Present`: the bucket
//! width and the present; `formats::ExportFormats`: the export formats the
//! backend writes). [`fixture::FixtureBackend`] (the `crosstalk-fixture`
//! crate) implements the spec's three with native `async fn`s, and
//! [`fixture`] implements the two gaps over its inherent methods. `app::AppBackend` names it, so every future a page
//! awaits has a concrete type and its `Send`-ness, which Topcoat's
//! multi-threaded runtime needs, is inferred where each `#[page]`, shard
//! and `#[route]` is registered.
//!
//! Implementations enforce permissions themselves and return
//! `QueryError::Forbidden`; pages also check, to render the "content
//! hidden" state instead of an error.

pub mod alert_state;
pub mod fixture;

use crosstalk_spec::interfaces::l8_surface::QueryError;

/// What the fixture's reads return: the spec's `QueryError` on failure.
pub type Result<T> = std::result::Result<T, QueryError>;
