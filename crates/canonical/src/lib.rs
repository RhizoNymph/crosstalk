//! L1 canonicalization for crosstalk: per-protocol normalizers from a raw
//! exchange to canonical exchanges and messages.
//!
//! Implements [`crosstalk_spec::interfaces::l1_canonical`]:
//!
//! - [`AnthropicMessages`] is the [`Normalizer`] for Anthropic Messages, as
//!   pure functions: a [`RawExchange`] in, a [`NormalizedExchange`] (or a
//!   [`Normalization`], which adds the media bytes) out, with streaming
//!   reassembly, tool calls and results, system prompts, cache control and
//!   thinking blocks.
//! - [`encoding`] is the canonical encoding of a message body and its
//!   [`MessageHash`] (BLAKE3 of the encoding, the blob store's key).
//! - [`json`] parses JSON with exact numbers and writes [`CanonicalJson`].
//! - [`capture::store`] writes a normalization's bodies through the spec's
//!   [`BlobStore`].
//!
//! Normalization reads no clock, randomness or node state and iterates no
//! hash map, so the same raw exchange always gives the same result
//! (`canonical.normalize.deterministic`).
//!
//! Roadmap: P2.5 (L1 canonical: Anthropic Messages normalizer). A layer crate:
//! it depends on the spec, never on another layer crate.
//!
//! [`Normalizer`]: crosstalk_spec::interfaces::l1_canonical::Normalizer
//! [`RawExchange`]: crosstalk_spec::interfaces::l0_ingress::RawExchange
//! [`NormalizedExchange`]: crosstalk_spec::interfaces::l1_canonical::NormalizedExchange
//! [`MessageHash`]: crosstalk_spec::ids::MessageHash
//! [`CanonicalJson`]: crosstalk_spec::observed::message::CanonicalJson
//! [`BlobStore`]: crosstalk_spec::interfaces::l2_transport::BlobStore

pub mod anthropic;
mod assemble;
pub mod capture;
pub mod encoding;
pub mod json;
pub mod sse;

pub use anthropic::AnthropicMessages;
pub use assemble::{MediaBlob, Normalization};
pub use capture::{StoreError, store};

#[cfg(test)]
mod tests;
