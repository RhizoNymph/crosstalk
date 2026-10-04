//! Tests for the invariants the spec's types enforce at runtime: checked
//! constructors, pattern matching, policy routing and event subjects.
//! Invariants that the type structure enforces need no test; they cannot be
//! violated.

mod fixtures;

mod aggregates;
mod events;
mod flow;
mod infrastructure;
mod observed;
mod provenance;
mod series;
mod support;
mod topic_history;
