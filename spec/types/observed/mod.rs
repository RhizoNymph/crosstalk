//! Observed tier: facts taken from the wire.
//!
//! Nothing here is inferred, except `Agent`, which is the one identity claim
//! the system has to make before it can attribute anything. An agent's
//! identity rests on [`agent::IdentityEvidence`], and merges are recorded
//! rather than rewritten.
//!
//! Invariants:
//! - A [`message::Message`] never changes after it is hashed. Its
//!   [`crate::ids::MessageHash`] is the BLAKE3 of its canonical encoding
//!   ([`message::encoding`]), which every layer that reads a body decodes.
//! - An [`exchange::Exchange`] references messages by hash only; bodies live
//!   in the blob store.

pub mod agent;
pub mod client;
pub mod conversation;
pub mod exchange;
pub mod message;
