//! Where L3 takes the ids it creates: merge records (the agent store),
//! agents and conversations (the consumer and the threader), and the
//! envelope ids of the events it publishes.
//!
//! Ids are minted at a time the caller passes (a merge's `at`, an
//! exchange's start), never at a clock reading.
//!
//! The stores' list cursor keys are derived from the deployment secret
//! ([`cursor_key`]), one label per list, so a cursor issued before a
//! restart still resolves after it (`surface.cursor.survives-restart`).

use std::sync::{Mutex, PoisonError};

use crosstalk_spec::ids::mint::{RandomSource, UlidExhausted, UlidGenerator};
use crosstalk_spec::ids::{EntityId, EventId, ExchangeId, KeyedHasher};
use crosstalk_spec::support::{Blake3, Timestamp};

/// A source of ids of type `I`, stamped with the time of what they name.
pub trait IdSource<I>: Send + Sync {
    /// The next id, stamped `at`. Every id is greater than the one before.
    fn next_id(&self, at: Timestamp) -> Result<I, UlidExhausted>;
}

/// [`IdSource`] over the spec's ULID generator, behind a mutex so a store
/// shared between tasks draws from one monotonic generator.
pub struct UlidSource<R> {
    generator: Mutex<UlidGenerator<R>>,
}

impl<R> UlidSource<R> {
    pub fn new(generator: UlidGenerator<R>) -> Self {
        Self {
            generator: Mutex::new(generator),
        }
    }
}

impl<R> std::fmt::Debug for UlidSource<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UlidSource").finish_non_exhaustive()
    }
}

impl<I: EntityId, R: RandomSource> IdSource<I> for UlidSource<R> {
    fn next_id(&self, at: Timestamp) -> Result<I, UlidExhausted> {
        // A poisoned lock only means another minting call panicked; the
        // generator's state (its last id) is still valid.
        let mut generator = self
            .generator
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        generator.mint_at(at)
    }
}

/// The random bits of a ULID.
const RANDOM_BITS: u32 = 80;

/// An envelope id that is a function of `exchange` and `kind`: every
/// delivery of one exchange's `ExchangeCaptured` publishes its L3 events
/// under the same ids, so consumers deduplicate them
/// (`reconstruct.delta.single-envelope-per-exchange`). The id keeps the
/// exchange id's millisecond, so it sorts with the exchange, and its random
/// part is a BLAKE3 of the kind and the exchange id.
pub fn derived_event_id(exchange: ExchangeId, kind: &str, salt: &[u8]) -> EventId {
    let raw = exchange.as_ulid();
    let millis = raw >> RANDOM_BITS;
    let mut bytes = b"crosstalk.reconstruct.envelope.v1/".to_vec();
    bytes.extend_from_slice(kind.as_bytes());
    bytes.push(b'/');
    bytes.extend_from_slice(&raw.to_be_bytes());
    bytes.push(b'/');
    bytes.extend_from_slice(salt);
    let digest = Blake3::of(&bytes);
    let mut random = [0u8; 16];
    random[6..].copy_from_slice(&digest.as_bytes()[..10]);
    let random = u128::from_be_bytes(random);
    EventId::from_ulid((millis << RANDOM_BITS) | random)
}

/// The label `PgAgents`' list cursor key is derived under.
pub const AGENTS_CURSOR_LABEL: &str = "crosstalk.cursor.v1.agents";

/// The label the conversation stores' list cursor key is derived under.
pub const CONVERSATIONS_CURSOR_LABEL: &str = "crosstalk.cursor.v1.conversations";

/// The cursor key for the list `label` names, derived from `secret`'s
/// current version (`KeyedHasher::derive_key`): the same on every start and
/// every node with that secret, different per label. A rotation changes it,
/// invalidating outstanding cursors (decision Q4 of postgres_stores.md).
pub fn cursor_key(secret: &KeyedHasher, label: &'static str) -> [u8; 32] {
    *secret.derive_key(label).as_bytes()
}
