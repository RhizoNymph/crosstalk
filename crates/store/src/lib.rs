//! Postgres infrastructure for crosstalk: the connection pool, per-layer schema
//! migrations and the test database harness.
//!
//! Hosts the Postgres implementations of the store traits in
//! [`crosstalk_spec::interfaces`]; each layer owns its own schema and
//! migrations.
//!
//! Roadmap: P1.5 (`crosstalk-store`). Infrastructure: layer crates may depend
//! on it.

// Every crate builds on the spec; the dependency is declared before any
// code uses it.
use crosstalk_spec as _;

#[cfg(test)]
mod tests {}
