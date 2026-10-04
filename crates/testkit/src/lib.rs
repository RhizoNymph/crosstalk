//! Test support for crosstalk: builders for common spec values, the
//! recorded-traffic corpus and its loader, and fake upstreams that replay it.
//!
//! - [`ids`] and [`time`]: deterministic ids from a seeded counter, and a
//!   fixed epoch ([`time::T0`]) for timestamps.
//! - [`build`]: builders for agents, resources, accesses, channels,
//!   exchanges and normalized exchanges, content matches, transmissions in
//!   every state, alerts, rules, topic versions, bus events and envelopes.
//!   Every value they return passes the spec's checked constructors.
//! - [`corpus`]: recorded Anthropic Messages traffic (synthetic for now,
//!   see `corpus/README.md`) under `corpus/`, loaded and checked by
//!   [`corpus::anthropic::cases`].
//! - [`upstream`]: [`upstream::FakeUpstream`], a hyper server that replays
//!   corpus responses, paces event streams, stalls, disconnects or fails on
//!   command, and records what it received.
//! - [`client`]: [`client::HarnessClient`], a hyper client that sends a
//!   corpus request and collects the response with chunk arrival times.
//!
//! Builds values of the types in [`crosstalk_spec`] and serves recorded
//! traffic to the L0 ingress interface in
//! [`crosstalk_spec::interfaces::l0_ingress`].
//!
//! Roadmap: P1.4 (`crosstalk-testkit`). A dev-dependency of the layer crates,
//! never a normal one.

pub mod build;
pub mod client;
pub mod corpus;
pub mod ids;
pub mod time;
pub mod upstream;

#[cfg(test)]
mod tests;
