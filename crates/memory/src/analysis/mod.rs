//! L6 reference stores ([`crosstalk_spec::interfaces::l6_analysis`]).
//!
//! - `TopicCatalog` and `TopicLifecycle` are
//!   [`catalog::InMemoryTopicCatalog`]: the fit lifecycle and the topic
//!   assignments.
//! - `SearchIndex` and `SearchCorpus` are [`search::InMemorySearchIndex`],
//!   exact search over the stored text and embeddings, with the analyze
//!   consumer's verdict copy; `ProjectionSource` is [`search::InMemoryProjectionSource`] over
//!   the same documents.
//! - `ProjectionStore` is [`projection::InMemoryProjectionStore`].
//! - `AlertRuleStore`, `AlertTriage`, `AlertRuleMaintenance`,
//!   `AlertActions` and `AlertReads` are all [`alerts::InMemoryAlertStore`],
//!   one transaction scope.
//! - The computational traits (`Embedder`, `TopicModel`, `LayoutFitter`,
//!   `RuleContext`) have deterministic `Fake*` doubles in [`fakes`].
//! - Merges and supersessions a test sets are [`aliases::StaticDirectory`].
//! - The similarity every score uses is in [`support`]; the building
//!   blocks every store shares (ids, the outbox, locks, cursors) are in
//!   [`crate::support`].

pub mod alerts;
pub mod aliases;
pub mod catalog;
pub mod fakes;
pub mod lineage;
pub mod projection;
pub mod search;
pub mod support;

#[cfg(test)]
pub(crate) mod tests;
