//! Ids derived from what they name, so a redelivered input, a replay or a
//! restarted shard mints the same id again and every store write the id
//! keys is idempotent.
//!
//! A derived id is a ULID: its 48-bit time part is the named thing's time
//! in milliseconds (so ids still sort by time), its 80 random bits the
//! first 80 bits of a BLAKE3 digest of a domain tag and the identity.

use crosstalk_spec::ids::mint::MAX_ULID_MILLIS;
use crosstalk_spec::ids::{AgentId, ExchangeId, TransmissionId};
use crosstalk_spec::support::Timestamp;

use super::key::MediumKey;
use super::route::RouteKey;

/// A digest of the parts written to it, as a ULID stamped at a time.
pub(crate) struct Derive {
    hasher: blake3::Hasher,
}

impl Derive {
    /// A derivation in `domain`, which keeps ids of different kinds apart
    /// even when their parts are equal.
    pub(crate) fn new(domain: &str) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(domain.as_bytes());
        hasher.update(&[0]);
        Self { hasher }
    }

    pub(crate) fn bytes(mut self, bytes: &[u8]) -> Self {
        // Length-prefixed, so two different part lists never hash alike.
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        self.hasher.update(&len.to_le_bytes());
        self.hasher.update(bytes);
        self
    }

    pub(crate) fn ulid(self, raw: u128) -> Self {
        self.bytes(&raw.to_le_bytes())
    }

    pub(crate) fn number(self, n: u64) -> Self {
        self.bytes(&n.to_le_bytes())
    }

    /// The ULID: `at`'s millisecond, then 80 bits of the digest.
    pub(crate) fn at(self, at: Timestamp) -> u128 {
        let digest = self.hasher.finalize();
        let mut random = [0_u8; 16];
        random[6..].copy_from_slice(&digest.as_bytes()[..10]);
        let millis = (at.as_micros() / 1000).min(MAX_ULID_MILLIS);
        (u128::from(millis) << 80) | u128::from_be_bytes(random)
    }
}

/// The identity of a transmission (`flow.transmission.identity`): its
/// reader exchange, its sender and its route. A channel transmission's
/// route is the medium it opened in (a resource before its channel was
/// discovered). `generation` counts the transmissions of this identity
/// already discarded: content arriving after a discard opens a new
/// transmission (`flow.transmission.content-after-discard-opens-new`).
pub(crate) fn transmission_id(
    opened_at: Timestamp,
    exchange: ExchangeId,
    sender: AgentId,
    route: &RouteKey,
    generation: u32,
) -> TransmissionId {
    let derive = Derive::new("crosstalk.flow.transmission")
        .ulid(exchange.as_ulid())
        .ulid(sender.as_ulid());
    let derive = route.write(derive).number(u64::from(generation));
    TransmissionId::from_ulid(derive.at(opened_at))
}

/// The route key of a channel transmission opened in `medium`.
pub(crate) fn medium_route(medium: MediumKey) -> RouteKey {
    RouteKey::Medium(medium)
}
