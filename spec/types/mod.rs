//! Crosstalk type specification.
//!
//! These modules are the design contract for the gateway's data model. They
//! are real Rust, type-checked and tested by `spec/Cargo.toml`, but they are
//! not part of the gateway's build: implementation crates copy or re-derive
//! them and add serde, sqlx and thiserror derives as needed.
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
//! or integers.

#![allow(async_fn_in_trait)]

pub mod aggregates;
pub mod derived;
pub mod events;
pub mod ids;
pub mod interfaces;
pub mod observed;
pub mod support;

#[cfg(test)]
mod tests;
