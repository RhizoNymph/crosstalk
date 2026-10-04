//! What the UI needs from L8 that `crosstalk-spec` does not have yet.
//!
//! Each item is numbered as in "The L8 contract" in `docs/features/ui.md`.
//! Names and shapes are the ones the UI asks the gateway for. Types that
//! replace a spec type of the same name (`TopologyFilter`, `OperatorAction`,
//! `QueryError`, `Alert`, `AgentState`, …) say so. When the gateway's types land, this module is
//! deleted and its users import those instead.

pub mod actions;
pub mod agents;
pub mod alerts;
pub mod channels;
pub mod evidence;
pub mod graph;
pub mod lists;
pub mod research;
pub mod rules;
pub mod scope;
pub mod search;
pub mod topics;
pub mod verdict;
