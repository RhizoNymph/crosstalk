//! Deterministic ids for test values.
//!
//! Every builder takes its ids from an [`Ids`]: a counter under a seed, so
//! the same sequence of builder calls under the same seed yields the same
//! values, and two generators with different seeds never collide.
//!
//! Entity ids are ULIDs whose 48-bit time part is [`Ids::TIME_MS`] (the
//! testkit epoch, [`crate::time::T0`]) and whose 80 random bits are the seed
//! (32 bits) above the counter (48 bits). The time part is never zero, so no
//! generated id falls in the range reserved for built-in alert rules
//! (`is_reserved_rule_id`). Digests (message hashes, credential and account
//! hashes) are 32 bytes expanded from the same counter.

use crosstalk_spec::ids::{
    AccessId, AccountHash, AgentId, AlertId, AlertRuleId, AuditId, ChannelId, ConversationId,
    CredentialHash, EventId, ExchangeId, ExportId, MergeId, MessageHash, OperatorId, ProjectionId,
    PromptHash, ResourceId, SecretVersion, SinkId, SpanId, TopicId, TransmissionId,
};
use crosstalk_spec::observed::exchange::ConnectionId;
use crosstalk_spec::support::Blake3;

/// An entity id a generator can mint: every spec entity id, and the
/// proxy's [`ConnectionId`].
pub trait EntityId: Copy {
    fn from_raw(raw: u128) -> Self;
}

macro_rules! entity_ids {
    ($($name:ident),* $(,)?) => {$(
        impl EntityId for $name {
            fn from_raw(raw: u128) -> Self {
                Self::from_ulid(raw)
            }
        }
    )*};
}

entity_ids!(
    AgentId,
    MergeId,
    ProjectionId,
    SinkId,
    ConversationId,
    ExchangeId,
    SpanId,
    ResourceId,
    AccessId,
    ChannelId,
    TransmissionId,
    TopicId,
    AlertRuleId,
    AlertId,
    OperatorId,
    EventId,
    AuditId,
    ExportId,
);

impl EntityId for ConnectionId {
    fn from_raw(raw: u128) -> Self {
        Self(raw)
    }
}

/// The secret version every generated credential and account hash claims.
pub const SECRET_VERSION: SecretVersion = SecretVersion(1);

/// A deterministic id generator. Not shared between tasks: a test owns one
/// and hands `&mut` to each builder it constructs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ids {
    seed: u32,
    next: u64,
}

impl Default for Ids {
    fn default() -> Self {
        Self::new()
    }
}

impl Ids {
    /// The ULID time part of every generated id: 2026-10-01T00:00:00Z in
    /// milliseconds, the same instant as [`crate::time::T0`].
    pub const TIME_MS: u64 = 1_790_812_800_000;

    /// The counter's width: 2^48 ids per seed.
    const COUNTER_BITS: u32 = 48;

    /// A generator under seed 0.
    pub fn new() -> Self {
        Self::seeded(0)
    }

    /// A generator under `seed`. Generators under different seeds never
    /// produce the same id or digest.
    pub fn seeded(seed: u32) -> Self {
        Self { seed, next: 0 }
    }

    pub fn seed(&self) -> u32 {
        self.seed
    }

    /// How many ids and digests this generator has handed out.
    pub fn issued(&self) -> u64 {
        self.next
    }

    /// The next 128-bit value: time part, seed, counter.
    fn raw(&mut self) -> u128 {
        self.next += 1;
        let counter = u128::from(self.next) & ((1u128 << Self::COUNTER_BITS) - 1);
        (u128::from(Self::TIME_MS) << 80) | (u128::from(self.seed) << Self::COUNTER_BITS) | counter
    }

    /// The next id of any entity type.
    pub fn id<I: EntityId>(&mut self) -> I {
        I::from_raw(self.raw())
    }

    pub fn agent(&mut self) -> AgentId {
        self.id()
    }

    pub fn exchange(&mut self) -> ExchangeId {
        self.id()
    }

    pub fn conversation(&mut self) -> ConversationId {
        self.id()
    }

    pub fn span(&mut self) -> SpanId {
        self.id()
    }

    pub fn resource(&mut self) -> ResourceId {
        self.id()
    }

    pub fn access(&mut self) -> AccessId {
        self.id()
    }

    pub fn channel(&mut self) -> ChannelId {
        self.id()
    }

    pub fn transmission(&mut self) -> TransmissionId {
        self.id()
    }

    pub fn topic(&mut self) -> TopicId {
        self.id()
    }

    pub fn alert(&mut self) -> AlertId {
        self.id()
    }

    pub fn rule(&mut self) -> AlertRuleId {
        self.id()
    }

    pub fn operator(&mut self) -> OperatorId {
        self.id()
    }

    pub fn event(&mut self) -> EventId {
        self.id()
    }

    pub fn merge(&mut self) -> MergeId {
        self.id()
    }

    /// The next 32-byte digest: the 128-bit value, mixed out to 32 bytes.
    /// Distinct for every call and every seed.
    pub fn digest(&mut self) -> Blake3 {
        Blake3::from_bytes(expand(self.raw()))
    }

    /// A message hash that names no particular content. For a hash derived
    /// from a message body use [`crate::build::message::content_hash`].
    pub fn message(&mut self) -> MessageHash {
        MessageHash::from_digest(self.digest())
    }

    pub fn prompt(&mut self) -> PromptHash {
        PromptHash::from_digest(self.digest())
    }

    /// A credential hash under [`SECRET_VERSION`].
    pub fn credential(&mut self) -> CredentialHash {
        CredentialHash::from_keyed_digest(SECRET_VERSION, self.digest())
    }

    /// An account hash under [`SECRET_VERSION`].
    pub fn account(&mut self) -> AccountHash {
        AccountHash::from_keyed_digest(SECRET_VERSION, self.digest())
    }
}

/// 32 bytes from a 128-bit value: the value itself (so distinct inputs give
/// distinct outputs), then a splitmix64 stream seeded by it (so digests do
/// not look like counters).
pub(crate) fn expand(raw: u128) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&raw.to_be_bytes());
    let mut state = (raw as u64) ^ ((raw >> 64) as u64);
    for chunk in out[16..].chunks_mut(8) {
        state = splitmix64(state);
        chunk.copy_from_slice(&state.to_be_bytes());
    }
    out
}

/// One step of splitmix64.
pub(crate) fn splitmix64(state: u64) -> u64 {
    let mut z = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
