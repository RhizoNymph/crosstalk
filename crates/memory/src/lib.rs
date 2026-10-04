//! In-memory reference implementations of every stateful store trait in the
//! crosstalk spec.
//!
//! Implements the store traits of
//! [`crosstalk_spec::interfaces::l3_reconstruction`],
//! [`crosstalk_spec::interfaces::l4_provenance`],
//! [`crosstalk_spec::interfaces::l5_flow`],
//! [`crosstalk_spec::interfaces::l6_analysis`],
//! [`crosstalk_spec::interfaces::l7_topology`] and
//! [`crosstalk_spec::interfaces::l8_surface`]. They are the models for the
//! Postgres stores' model-based tests and the stores behind the simulation.
//!
//! Roadmap: P2.3 (`crosstalk-memory`: reference stores). A dev-dependency of
//! the layer crates, never a normal one.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

pub mod analysis;
pub mod flow;
pub mod model;
pub mod pipeline;
pub mod provenance;
pub mod reconstruct;
pub mod surface;
pub mod topology;

#[cfg(test)]
mod tests {}
