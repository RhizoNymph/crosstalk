//! Deterministic ids.
//!
//! Every id the corpus or the reference matcher mints is derived from what it
//! names, never drawn at random, so a re-run over the same data is
//! byte-identical. An id is the first 16 bytes (big-endian) of
//!
//! ```text
//! BLAKE3("crosstalk-eval/v1" 0x00 kind 0x00 dataset 0x00 part_1 0x00 … part_n)
//! ```
//!
//! and, for ids of things that happen at a time (exchanges), the top 48 bits
//! are replaced by that time in milliseconds, as a ULID's are, so such ids
//! sort by time like the gateway's.

use crosstalk_spec::ids::{
    AgentId, ChannelId, ConversationId, ExchangeId, ResourceId, SpanId, TransmissionId,
};
use crosstalk_spec::support::{Blake3, Timestamp};

use crate::keys::{AgentKey, DatasetId, SourceRef};

const DOMAIN: &str = "crosstalk-eval/v1";

/// The 32-byte digest of `kind`, `dataset` and `parts`, each separated by a
/// zero byte.
pub fn digest(kind: &str, dataset: &DatasetId, parts: &[&str]) -> Blake3 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN.as_bytes());
    for field in [kind, dataset.as_str()]
        .into_iter()
        .chain(parts.iter().copied())
    {
        hasher.update(&[0]);
        hasher.update(field.as_bytes());
    }
    Blake3::from_bytes(*hasher.finalize().as_bytes())
}

/// The id bits: the digest's first 16 bytes, with the top 48 bits replaced by
/// `at` in milliseconds when given.
pub fn derive(kind: &str, dataset: &DatasetId, parts: &[&str], at: Option<Timestamp>) -> u128 {
    let bytes = digest(kind, dataset, parts);
    let mut head = [0u8; 16];
    head.copy_from_slice(&bytes.as_bytes()[..16]);
    let raw = u128::from_be_bytes(head);
    match at {
        None => raw,
        Some(at) => {
            let millis = u128::from(at.as_micros() / 1000) & ((1u128 << 48) - 1);
            (millis << 80) | (raw & ((1u128 << 80) - 1))
        }
    }
}

pub fn agent_id(dataset: &DatasetId, agent: &AgentKey) -> AgentId {
    AgentId::from_ulid(derive(
        "agent",
        dataset,
        &[agent.world.as_str(), &agent.name],
        None,
    ))
}

pub fn exchange_id(dataset: &DatasetId, source: &SourceRef, at: Timestamp) -> ExchangeId {
    ExchangeId::from_ulid(derive(
        "exchange",
        dataset,
        &[&source.file, &source.path],
        Some(at),
    ))
}

pub fn conversation_id(dataset: &DatasetId, agent: &AgentKey, scope: &str) -> ConversationId {
    ConversationId::from_ulid(derive(
        "conversation",
        dataset,
        &[agent.world.as_str(), &agent.name, scope],
        None,
    ))
}

/// A span, by the exchange holding it and its location text.
pub fn span_id(dataset: &DatasetId, exchange: ExchangeId, location: &str) -> SpanId {
    SpanId::from_ulid(derive(
        "span",
        dataset,
        &[&exchange.ulid_text(), location],
        None,
    ))
}

/// A transmission, by its identity: reader exchange, sender and route.
pub fn transmission_id(
    dataset: &DatasetId,
    reader_exchange: ExchangeId,
    sender: AgentId,
    route: &str,
) -> TransmissionId {
    TransmissionId::from_ulid(derive(
        "transmission",
        dataset,
        &[&reader_exchange.ulid_text(), &sender.ulid_text(), route],
        None,
    ))
}

pub fn channel_id(dataset: &DatasetId, resource: &str) -> ChannelId {
    ChannelId::from_ulid(derive("channel", dataset, &[resource], None))
}

pub fn resource_id(dataset: &DatasetId, resource: &str) -> ResourceId {
    ResourceId::from_ulid(derive("resource", dataset, &[resource], None))
}
