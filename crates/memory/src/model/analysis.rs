//! Harnesses for the L6 stores.
//!
//! | Harness | Trait | Subject |
//! | --- | --- | --- |
//! | [`check_topic_catalog`] | `TopicCatalog` | [`CatalogSubject`] |
//! | [`check_search_index`] | `SearchIndex` (and `ProjectionSource`) | [`SearchSubject`] |
//! | [`check_projection_store`] | `ProjectionStore` | the trait itself |
//! | [`check_alert_rule_store`] | `AlertRuleStore` | [`AlertStoreSubject`] |
//! | [`check_alert_triage`] | `AlertTriage` | [`AlertStoreSubject`] |
//!
//! A subject trait adds to the spec trait the writes the spec leaves to the
//! implementation (the fit lifecycle, indexing, the consumer's rule
//! changes) and the world the store reads at query time (merges,
//! supersessions, the catalog), so the harness can drive both sides
//! identically.

mod alerts;
mod catalog;
mod projection;
mod search;

pub use alerts::{
    AlertStoreSubject, AlertWorld, ReferenceAlerts, alert_world, check_alert_rule_store,
    check_alert_triage,
};
pub use catalog::{CatalogSubject, ReferenceCatalog, check_topic_catalog};
pub use projection::{check_projection_store, projection_config};
pub use search::{
    FilterSeed, ReferenceSearch, SearchSubject, check_search_index, filter_seed, harness_model,
    new_version_in,
};
