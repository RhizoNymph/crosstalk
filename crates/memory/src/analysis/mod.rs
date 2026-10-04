//! L6 reference stores ([`crosstalk_spec::interfaces::l6_analysis`]).
//!
//! - `TopicCatalog` is [`catalog::InMemoryTopicCatalog`], with the fit
//!   lifecycle and the topic assignments.
//! - `SearchIndex` is [`search::InMemorySearchIndex`], exact search over
//!   the stored text and embeddings, with the analyze consumer's verdict
//!   copy; `ProjectionSource` is [`search::InMemoryProjectionSource`] over
//!   the same documents.
//! - `ProjectionStore` is [`projection::InMemoryProjectionStore`].
//! - `AlertRuleStore` and `AlertTriage` are both
//!   [`alerts::InMemoryAlertStore`], one transaction scope, with the alert
//!   reads and acknowledgements.
//! - The computational traits (`Embedder`, `TopicModel`, `LayoutFitter`,
//!   `RuleContext`) have deterministic `Fake*` doubles in [`fakes`].
//! - Merges and supersessions a test sets are [`aliases::StaticDirectory`].
//! - The clock, id sequences, the outbox and the similarity every score
//!   uses are in [`support`].

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
