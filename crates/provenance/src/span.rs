//! Deterministic ids: a span's id and the id of every envelope provenance
//! publishes are functions of what they describe, so a redelivered delta
//! yields the same spans and the same envelopes
//! (`provenance.delta.redelivery-idempotent`), which consumers deduplicate.
//!
//! Each id is a ULID whose 48-bit time is the exchange id's (so ids sort
//! by exchange time like minted ones) and whose 80 random bits are the
//! leading bits of a BLAKE3 digest over a domain tag and the describing
//! fields.

use crosstalk_spec::derived::provenance::span::SpanLocation;
use crosstalk_spec::ids::{EventId, ExchangeId, SpanId};
use crosstalk_spec::support::Blake3;

const RANDOM_BITS: u32 = 80;

fn derive(exchange: ExchangeId, tag: &[u8], fields: &[&[u8]]) -> u128 {
    let mut bytes = Vec::with_capacity(64);
    bytes.extend_from_slice(b"crosstalk.provenance.");
    bytes.extend_from_slice(tag);
    bytes.push(0);
    bytes.extend_from_slice(&exchange.as_ulid().to_be_bytes());
    for field in fields {
        bytes.extend_from_slice(&u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        bytes.extend_from_slice(field);
    }
    let digest = Blake3::of(&bytes);
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&digest.as_bytes()[..10]);
    let random = u128::from_be_bytes(random);
    let time = exchange.as_ulid() >> RANDOM_BITS << RANDOM_BITS;
    time | random
}

fn location_fields(location: &SpanLocation) -> [Vec<u8>; 4] {
    [
        location.part.message.digest().as_bytes().to_vec(),
        location.part.index.to_be_bytes().to_vec(),
        location.range.start().to_be_bytes().to_vec(),
        location.range.end().to_be_bytes().to_vec(),
    ]
}

/// The id of the span at `location` in `exchange`'s output.
pub fn span_id(exchange: ExchangeId, location: &SpanLocation) -> SpanId {
    let fields = location_fields(location);
    let refs: Vec<&[u8]> = fields.iter().map(Vec::as_slice).collect();
    SpanId::from_ulid(derive(exchange, b"span", &refs))
}

/// The envelope id announcing the span `span` (`SpanOriginated` or
/// `SpanRelayed`).
pub fn span_event_id(exchange: ExchangeId, span: SpanId) -> EventId {
    EventId::from_ulid(derive(
        exchange,
        b"span-event",
        &[&span.as_ulid().to_be_bytes()],
    ))
}

/// The id of the content match of `origin` read at `read_at` in
/// `exchange`, which is also its envelope's id.
pub fn match_id(exchange: ExchangeId, origin: SpanId, read_at: &SpanLocation) -> EventId {
    let fields = location_fields(read_at);
    let mut refs: Vec<&[u8]> = fields.iter().map(Vec::as_slice).collect();
    let origin = origin.as_ulid().to_be_bytes();
    refs.push(&origin);
    EventId::from_ulid(derive(exchange, b"match", &refs))
}
