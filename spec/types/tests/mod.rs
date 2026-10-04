//! Tests for the invariants the spec's types enforce at runtime: checked
//! constructors, pattern matching, policy routing, event subjects and the
//! shared view filter. Invariants that the type structure enforces need no
//! test; they cannot be violated.

mod fixtures;

mod aggregates;
mod audit;
mod events;
mod filter;
mod flow;
mod infrastructure;
mod live;
mod observed;
mod paging;
mod policy;
mod projection;
mod provenance;
mod series;
mod support;
mod topic_history;
