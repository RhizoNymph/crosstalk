//! L8 surface for crosstalk: the query API, operator actions, the live feed and
//! export, generic over the spec's store traits.
//!
//! Implements [`crosstalk_spec::interfaces::l8_surface`].
//!
//! Roadmap: P2.6 (L8 surface over spec traits) and P7.3 (surface on Postgres).
//! A layer crate: it depends on the spec, never on another layer crate.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
