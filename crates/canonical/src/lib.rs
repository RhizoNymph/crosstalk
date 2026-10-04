//! L1 canonicalization for crosstalk: per-protocol normalizers from a raw
//! exchange to canonical exchanges and messages.
//!
//! Implements [`crosstalk_spec::interfaces::l1_canonical`].
//!
//! Roadmap: P2.5 (L1 canonical: Anthropic Messages normalizer). A layer crate:
//! it depends on the spec, never on another layer crate.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
