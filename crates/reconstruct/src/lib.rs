//! L3 reconstruction for crosstalk: agent identity, merges and unmerges,
//! harness claims and conversation threading.
//!
//! Implements [`crosstalk_spec::interfaces::l3_reconstruction`]:
//!
//! - [`evidence`]: the `EvidenceDeriver`s (credential and account, scoped
//!   harness ids, the prompt fingerprint, and their chain), and the one
//!   place an exchange's identity scope is decided ([`evidence::scope`]).
//! - [`agents`]: [`agents::PgAgents`], every L3 agent store trait on
//!   Postgres (the directory, the merge log with exact unmerges and vetoes,
//!   resolution, the lifecycle, claims, activity and the agent reads).
//! - [`thread`]: the `Threader` ([`thread::ConversationThreader`]) over a
//!   [`thread::ConversationStore`], in memory or on Postgres: prefix
//!   matching, forks, compaction and WebSocket increment resolution.
//! - [`consumer`]: the L3 bus consumer, which turns each
//!   `ExchangeCaptured` into an attribution, a threading and the L3 events.
//!
//! Roadmap: P4.1 (L3 reconstruction). A layer crate: it depends on the spec
//! and `crosstalk-store`, never on another layer crate. Every method that
//! depends on the time takes it as an argument.

pub mod agents;
pub mod consumer;
pub mod error;
pub mod evidence;
pub mod ids;
pub mod publish;
pub mod thread;

#[cfg(test)]
mod tests;
