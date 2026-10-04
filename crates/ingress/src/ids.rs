//! Keyed hashing and id minting, in one small module.
//!
//! This is a stand-in for the keyed hasher, `DeploymentSecret` and ULID
//! generator that roadmap item P0.7 moves into `crosstalk-spec` (the
//! `canonical.ids.*` invariants). It has their semantics, so it can be
//! swapped for theirs at merge:
//!
//! - a [`CredentialHash`] or [`AccountHash`] is `blake3::keyed_hash` of the
//!   raw value under the deployment secret of the [`SecretVersion`] it
//!   records; during a rotation overlap the previous version's digest is
//!   computed too;
//! - a [`DeploymentSecret`] never appears in `Debug` (redacted), has no
//!   `Display` and no serialization;
//! - exchange ids are ULIDs whose time comes from the injected clock's
//!   reading and whose random part never repeats within a generator.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crosstalk_spec::ids::{AccountHash, CredentialHash, ExchangeId, SecretVersion};
use crosstalk_spec::support::{Blake3, Timestamp};

/// The deployment secret that keys every digest: 32 bytes. Its `Debug` is
/// redacted; it has no `Display` and no serialization.
#[derive(Clone, PartialEq, Eq)]
pub struct DeploymentSecret([u8; 32]);

impl DeploymentSecret {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// 64 hex digits, either case, surrounding whitespace ignored.
    pub fn from_hex(text: &str) -> Result<Self, SecretFormat> {
        let text = text.trim();
        if text.len() != 64 {
            return Err(SecretFormat::Length);
        }
        let mut bytes = [0u8; 32];
        let (pairs, _) = text.as_bytes().as_chunks::<2>();
        for (index, [high, low]) in pairs.iter().enumerate() {
            let high = hex_value(*high).ok_or(SecretFormat::NotHex)?;
            let low = hex_value(*low).ok_or(SecretFormat::NotHex)?;
            bytes[index] = (high << 4) | low;
        }
        Ok(Self(bytes))
    }

    fn keyed(&self, value: &[u8]) -> Blake3 {
        Blake3::from_bytes(*blake3::keyed_hash(&self.0, value).as_bytes())
    }
}

impl fmt::Debug for DeploymentSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DeploymentSecret(<redacted>)")
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Why a secret's text is not 32 bytes of hex. Carries nothing of the text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SecretFormat {
    #[error("is not 64 hex digits long")]
    Length,
    #[error("contains a character that is not a hex digit")]
    NotHex,
}

/// Two loaded secrets share a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the current and previous secrets are both version {0:?}")]
pub struct SameVersion(pub SecretVersion);

/// The keyed hasher: the current secret, and the previous one during a
/// rotation overlap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyedHasher {
    current: (SecretVersion, DeploymentSecret),
    previous: Option<(SecretVersion, DeploymentSecret)>,
}

impl KeyedHasher {
    pub fn new(
        current: (SecretVersion, DeploymentSecret),
        previous: Option<(SecretVersion, DeploymentSecret)>,
    ) -> Result<Self, SameVersion> {
        if let Some((version, _)) = &previous
            && *version == current.0
        {
            return Err(SameVersion(*version));
        }
        Ok(Self { current, previous })
    }

    pub fn current_version(&self) -> SecretVersion {
        self.current.0
    }

    pub fn credential(&self, raw: &[u8]) -> CredentialHash {
        let (version, key) = &self.current;
        CredentialHash::from_keyed_digest(*version, key.keyed(raw))
    }

    pub fn account(&self, raw: &[u8]) -> AccountHash {
        let (version, key) = &self.current;
        AccountHash::from_keyed_digest(*version, key.keyed(raw))
    }

    /// The credential's digest under the previous version, during an
    /// overlap.
    pub fn previous_credential(&self, raw: &[u8]) -> Option<CredentialHash> {
        let (version, key) = self.previous.as_ref()?;
        Some(CredentialHash::from_keyed_digest(*version, key.keyed(raw)))
    }

    pub fn previous_account(&self, raw: &[u8]) -> Option<AccountHash> {
        let (version, key) = self.previous.as_ref()?;
        Some(AccountHash::from_keyed_digest(*version, key.keyed(raw)))
    }

    /// Whether a rotation overlap is in progress.
    pub fn in_overlap(&self) -> bool {
        self.previous.is_some()
    }
}

/// Mints exchange ids: ULIDs whose time is the clock reading passed in,
/// in milliseconds, and whose 80 low bits are a per-generator random base
/// plus a counter, so one generator never repeats an id.
#[derive(Debug)]
pub struct ExchangeIds {
    base: u128,
    counter: AtomicU64,
}

const LOW_80: u128 = (1 << 80) - 1;

impl ExchangeIds {
    /// A random base, from the standard library's per-process random hash
    /// keys.
    pub fn random() -> Self {
        use std::hash::{BuildHasher, RandomState};
        let state = RandomState::new();
        let high = u128::from(state.hash_one(0x6372_6f73_7374_616c_u64));
        let low = u128::from(state.hash_one(0x6b2e_696e_6772_6573_u64));
        Self::with_base((high << 64) | low)
    }

    /// A fixed base, for tests and simulations.
    pub fn seeded(seed: u64) -> Self {
        Self::with_base(u128::from(seed) << 16)
    }

    fn with_base(base: u128) -> Self {
        Self {
            base: base & LOW_80,
            counter: AtomicU64::new(0),
        }
    }

    /// The next id, stamped with `at` (read from the injected clock).
    pub fn next(&self, at: Timestamp) -> ExchangeId {
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        let millis = u128::from(at.as_micros() / 1000) & ((1 << 48) - 1);
        let random = self.base.wrapping_add(u128::from(count)) & LOW_80;
        ExchangeId::from_ulid((millis << 80) | random)
    }
}
