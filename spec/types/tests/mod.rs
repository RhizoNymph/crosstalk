//! Tests for the invariants the spec's types enforce at runtime: checked
//! constructors, pattern matching, policy routing and event subjects.
//! Invariants that the type structure enforces need no test; they cannot be
//! violated.

mod fixtures;

mod aggregates;
mod audit;
mod events;
mod flow;
mod infrastructure;
mod live;
mod observed;
mod policy;
mod provenance;
mod support;
