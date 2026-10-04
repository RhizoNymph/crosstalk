//! Deterministic simulation for crosstalk: a virtual clock, a seeded RNG, fault
//! injection for the bus, the store and upstreams, and the driver that replays
//! a scenario from a seed.
//!
//! Drives the layer traits in [`crosstalk_spec::interfaces`] under simulated
//! time and faults, for every invariant whose `requires` lists `dst`.
//!
//! Roadmap: P1.3 (`crosstalk-sim`). A dev-dependency of the layer crates, never
//! a normal one.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
