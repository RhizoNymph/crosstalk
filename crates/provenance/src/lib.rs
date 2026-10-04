//! L4 provenance for crosstalk: segmentation, decoders, fingerprinting and
//! content matching.
//!
//! Implements [`crosstalk_spec::interfaces::l4_provenance`]:
//!
//! - [`segment::NovelRunSegmenter`] (`Segmenter`): cuts an output into
//!   spans, relayed from an input or originated.
//! - [`decode`]: `Base64Decoder`, `HexDecoder`, `UrlDecoder`,
//!   `UnicodeNormalizer` (`Decoder`), the JSON and YAML string unescapes,
//!   and the depth-bounded [`decode::DecodePipeline`].
//! - [`fingerprint::Winnowing`] (`Fingerprinter`): winnowing over
//!   whitespace- and case-normalized shingles.
//! - [`index::PgFingerprintIndex`] (`FingerprintIndex`) on Postgres.
//! - [`semantic::DisabledSemanticMatcher`] (`SemanticMatcher`): a stub
//!   until embeddings exist.
//! - [`scan::Scanner`] and [`engine::Provenance`]: what a delta means, its
//!   spans and content matches, and the index writes; [`store`] keeps L4's
//!   records (exchanges and scan status, spans, matches) in memory or in
//!   Postgres.
//! - [`consumer`]: the bus consumer (group `provenance`) publishing
//!   `SpanOriginated`, `SpanRelayed` and `ContentMatched`.
//!
//! Configuration is typed ([`config::ProvenanceConfig`]); every time is an
//! argument or read from the injected clock.
//!
//! Roadmap: P4.2 (L4 provenance). A layer crate: it depends on the spec and
//! `crosstalk-store`, never on another layer crate.

pub mod config;
pub mod consumer;
pub mod decode;
pub mod engine;
pub mod fingerprint;
pub mod index;
mod pg;
pub mod scan;
pub mod segment;
pub mod semantic;
pub mod span;
pub mod store;
pub mod text;

#[cfg(test)]
mod props;
#[cfg(test)]
mod tests;
