//! L4 reference store: the fingerprint index.
//!
//! [`MemoryFingerprintIndex`] implements `FingerprintIndex`: postings from
//! fingerprints to the originated spans that contain them, frequency
//! observations of every scanned text, the boilerplate cutoff, retention
//! and shard ownership. It holds no text: postings are fingerprints, span
//! ids and offsets, and observations are fingerprints and times
//! (`provenance.index.no-text`).
//!
//! `SemanticMatcher` is not here: its lookup embeds query text, which is a
//! model, not a store.
//!
//! [`model`] is the model-based property harness the Postgres index reuses.

mod index;
pub mod model;

#[cfg(test)]
mod tests;

pub use index::{IndexConfig, InvalidIndexConfig, MemoryFingerprintIndex};
