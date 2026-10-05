//! Demo and load generation for crosstalk, without spending real tokens.
//!
//! - [`upstream`]: a fake Anthropic Messages upstream. `POST /v1/messages`
//!   in the real wire format, streaming or not, answered deterministically
//!   from a seed and the request body with text and `tool_use` blocks,
//!   after a configurable wait and with configurable stream pacing.
//! - [`wiki`]: a tiny in-memory HTTP page store, the shared channel the
//!   agents talk through and crosstalk should discover.
//! - [`swarm`]: N simulated agents with growing conversations, sent whole
//!   through the crosstalk proxy every turn, executing the model's wiki
//!   tool calls and sending back their results, so one agent's model
//!   output (a page it wrote) reaches another agent's input (the page read
//!   back as a tool result). Reports throughput and client-observed
//!   latency.
//!
//! The swarm and the fake model agree on a task marker closing each prompt
//! ([`protocol::Task`]), so the swarm's knobs decide how often the wiki is
//! written and read while the text still comes from the model.
//!
//! A tool crate (`crates/gateway/tests/architecture.rs`): no layer crate
//! depends on it. It reuses `crosstalk-testkit`'s harness client and SSE
//! parser and the spec's seeded random source.

pub mod anthropic;
pub mod cli;
pub mod http;
pub mod knobs;
pub mod logging;
pub mod protocol;
pub mod swarm;
pub mod upstream;
pub mod wiki;

#[cfg(test)]
mod tests;
