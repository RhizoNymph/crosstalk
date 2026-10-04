//! L3 reconstruction for crosstalk: agent identity, merges and unmerges,
//! harness claims and conversation threading.
//!
//! Implements [`crosstalk_spec::interfaces::l3_reconstruction`].
//!
//! Roadmap: P4.1 (L3 reconstruction). A layer crate: it depends on the spec,
//! never on another layer crate.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
