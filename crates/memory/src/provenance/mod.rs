//! L4 reference store: the fingerprint index.
//!
//! [`MemoryFingerprintIndex`] implements `FingerprintIndex`: postings from
//! fingerprints to the originated spans that contain them, frequency
//! observations of every scanned text, the boilerplate cutoff, retention
//! and shard ownership. It also implements `SpanIndex`: each recorded span's
//! exchange, author and location, read back in batches. It holds no text:
//! postings are fingerprints, span ids and offsets, observations are
//! fingerprints and times, and span records are ids and byte ranges
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
