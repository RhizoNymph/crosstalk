//! L7 topology for crosstalk: edge and access buckets, graphs, series, the
//! watermark and retention.
//!
//! Implements [`crosstalk_spec::interfaces::l7_topology`].
//!
//! Roadmap: P6.1 (L7 topology). A layer crate: it depends on the spec, never on
//! another layer crate.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
