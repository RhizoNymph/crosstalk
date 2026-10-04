//! Test support for crosstalk: builders for common spec values, the
//! recorded-traffic corpus and its loader, and fake upstreams that replay it.
//!
//! Builds values of the types in [`crosstalk_spec`] and serves recorded traffic
//! to the L0 ingress interface in [`crosstalk_spec::interfaces::l0_ingress`].
//!
//! Roadmap: P1.4 (`crosstalk-testkit`). A dev-dependency of the layer crates,
//! never a normal one.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
