//! L4 provenance for crosstalk: segmentation, decoders, fingerprinting and
//! content matching.
//!
//! Implements [`crosstalk_spec::interfaces::l4_provenance`].
//!
//! Roadmap: P4.2 (L4 provenance). A layer crate: it depends on the spec, never
//! on another layer crate.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
