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
//! A subject trait is the set of spec traits the harness drives, write side
//! included (the fit lifecycle, indexing, the consumer's rule upkeep, alert
//! actions and reads), so any store implementing the spec can be checked.
//! What a store reads from other layers' caches (merges and supersessions,
//! L7's watermark) is a world of spec read traits the harness hands `make`
//! and changes itself, for the subject and the reference alike.

mod alerts;
mod catalog;
mod projection;
mod search;

pub use alerts::{
    AlertStoreSubject, AlertWorld, ReferenceAlerts, alert_world, check_alert_rule_store,
    check_alert_triage, reference_alerts,
};
pub use catalog::{CatalogSubject, ReferenceCatalog, check_topic_catalog, reference_catalog};
pub use projection::{check_projection_store, projection_config};
pub use search::{
    FilterSeed, ReferenceSearch, SearchSubject, SearchWorld, check_search_index, filter_seed,
    harness_model, new_version_in,
};
