//! Crosstalk type specification.
//!
//! These modules are the design contract for the gateway's data model. They
//! are real Rust, type-checked and tested by `spec/Cargo.toml`, and they are
//! the shared boundary crate of the workspace: every implementation crate
//! depends on them, and layer crates reach each other only through them.
//! They are also the wire format: the types
//! serialize to the JSON the gateway, the operator UI and other gateway
//! nodes exchange, by the conventions in [`wire`], with golden files pinning
//! each shape. Implementation crates add sqlx and thiserror as needed.
//!
//! The model has three tiers, plus the events and interfaces that move data
//! between them:
//!
//! - [`observed`]: immutable facts taken from the wire. Messages are
//!   content-addressed. The only inference here is agent identity, which
//!   carries its evidence.
//! - [`derived`]: inferences. Every derived claim carries the evidence it
//!   rests on, and the types make a claim without evidence unrepresentable.
//! - [`aggregates`]: recomputable summaries (edges, topics, alerts). They can
//!   be dropped and rebuilt from the derived tier.
//! - [`events`]: what crosses the event bus between layers.
//! - [`interfaces`]: the trait each layer of the abstraction stack exposes,
//!   with its error type.
//!
//! Cross-tier references are always typed ids from [`ids`], never raw strings
//! or integers. Merged agents and superseded channels are aliases, resolved
//! at read time through [`aliases`]. List queries page with the opaque
//! cursors in [`paging`]. What a client may send is marked
//! [`wire::WireRequest`]; what the server stamps never is.

#![allow(async_fn_in_trait)]

pub mod aggregates;
pub mod aliases;
pub mod batch;
pub mod derived;
pub mod events;
pub mod ids;
pub mod interfaces;
pub mod observed;
pub mod paging;
pub mod support;
pub mod wire;

#[cfg(test)]
mod tests;
