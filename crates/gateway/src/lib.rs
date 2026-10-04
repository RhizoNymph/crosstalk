//! The crosstalk gateway: configuration, wiring and process roles behind the
//! `crosstalk` binary.
//!
//! Composes the implementations of every layer in
//! [`crosstalk_spec::interfaces`]; it is the one place layer crates are wired
//! together.
//!
//! Roadmap: P3 (capture slice) and P1.1 (workspace, which also holds the
//! architecture test). A composition crate: it may depend on layer crates.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
