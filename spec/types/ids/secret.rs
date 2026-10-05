//! The deployment secret and the keyed hasher behind [`CredentialHash`] and
//! [`AccountHash`].
//!
//! A secret digest is BLAKE3 in keyed mode, keyed with one version of the
//! deployment secret, and records that version
//! (`canonical.ids.secret-digest-keyed`,
//! `canonical.ids.secret-version-recorded`). Every node loads the same
//! versions, so the same credential digests to the same value on every
//! proxy node.
//!
//! **Rotation.** A new version becomes current while the previous one stays
//! loaded until its overlap ends. Inside the overlap [`KeyedHasher`]
//! returns the digest under each loaded version, the current one first, so
//! identity resolution can link an agent's evidence across the change;
//! after it, only the current one (`canonical.ids.rotation-overlap-digests`).
//! [`SecretDigests`] maps onto a client context: `current` is the
//! credential's or account's hash, `previous` goes into
//! [`PreviousDigests`].
//!
//! **Confidentiality.** A [`DeploymentSecret`]'s key has no accessor: only
//! this module's hasher reads it. Neither it nor [`KeyedHasher`] implements
//! `Serialize`, `Deserialize`, `Clone` or `PartialEq`, and their `Debug`
//! and `Display` print the version alone
//! (`canonical.ids.secret-never-emitted`,
//! `canonical.ids.secret-never-serialized`; the compile-time checks are in
//! `crate::wire::confidential`).
//!
//! [`PreviousDigests`]: crate::observed::client::PreviousDigests

use std::fmt;

use super::{AccountHash, CredentialHash, SecretVersion};
use crate::support::{Blake3, Timestamp};

/// The length of a deployment secret: one BLAKE3 key.
pub const SECRET_LEN: usize = 32;

/// One version of the deployment secret: a 32-byte BLAKE3 key.
///
/// Loaded from configuration (an environment variable holding 64 hex
/// digits, [`DeploymentSecret::from_hex`]) and handed to a [`KeyedHasher`].
/// Nothing outside this module can read the key back.
pub struct DeploymentSecret {
    version: SecretVersion,
    key: [u8; SECRET_LEN],
}

/// Why text is not a deployment secret. Carries positions and lengths
/// only, never the text, so reporting it reveals nothing of the secret.
/// Both count within the text with its surrounding ASCII whitespace
/// removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidSecret {
    /// Not 64 characters.
    Length { got: usize },
    /// The character at `index` is not a hex digit (either case).
    NotHex { index: usize },
}

impl fmt::Display for InvalidSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Length { got } => write!(
                f,
                "a deployment secret is {} hex digits, not {got} characters",
                2 * SECRET_LEN
            ),
            Self::NotHex { index } => {
                write!(f, "a deployment secret's character {index} is not hex")
            }
        }
    }
}

impl std::error::Error for InvalidSecret {}

impl DeploymentSecret {
    pub const fn new(version: SecretVersion, key: [u8; SECRET_LEN]) -> Self {
        Self { version, key }
    }

    /// The secret `text` spells: 64 hex digits, upper or lower case (a
    /// secret is configuration, not a wire value, so either case is read).
    /// Surrounding ASCII whitespace is ignored, since an environment
    /// variable or a mounted secret file often ends in a newline; an
    /// error's length and index are those of the trimmed text.
    pub fn from_hex(version: SecretVersion, text: &str) -> Result<Self, InvalidSecret> {
        let text = text.trim_ascii();
        let bytes = text.as_bytes();
        if bytes.len() != 2 * SECRET_LEN {
            return Err(InvalidSecret::Length {
                got: text.chars().count(),
            });
        }
        let nibble = |index: usize| match bytes[index] {
            digit @ b'0'..=b'9' => Ok(digit - b'0'),
            letter @ b'a'..=b'f' => Ok(letter - b'a' + 10),
            letter @ b'A'..=b'F' => Ok(letter - b'A' + 10),
            _ => Err(InvalidSecret::NotHex { index }),
        };
        let mut key = [0u8; SECRET_LEN];
        for (index, byte) in key.iter_mut().enumerate() {
            *byte = (nibble(2 * index)? << 4) | nibble(2 * index + 1)?;
        }
        Ok(Self { version, key })
    }

    pub const fn version(&self) -> SecretVersion {
        self.version
    }

    /// The keyed BLAKE3 of `raw` under this secret. Private: the hasher is
    /// the only reader of the key.
    fn digest(&self, raw: &[u8]) -> Blake3 {
        Blake3::from_bytes(*blake3::keyed_hash(&self.key, raw).as_bytes())
    }
}

/// The version only; the key is never formatted.
impl fmt::Debug for DeploymentSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeploymentSecret")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// `deployment secret version <n>`; the key is never formatted.
impl fmt::Display for DeploymentSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "deployment secret version {}", self.version.0)
    }
}

/// A rotation's previous secret and when its overlap ends.
#[derive(Debug)]
struct Overlap {
    previous: DeploymentSecret,
    /// Exclusive: the previous version keys digests while `at < ends`.
    ends: Timestamp,
}

/// Why a rotation's secrets do not form a key set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidRotation {
    /// The previous version is not older than the current one: versions
    /// only grow, so one number never names two keys.
    NotOlder {
        previous: SecretVersion,
        current: SecretVersion,
    },
}

impl fmt::Display for InvalidRotation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotOlder { previous, current } => write!(
                f,
                "previous secret version {} is not older than current version {}",
                previous.0, current.0
            ),
        }
    }
}

impl std::error::Error for InvalidRotation {}

/// A value's digest under each loaded secret version, the current one
/// first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretDigests<D> {
    /// Under the current version.
    pub current: D,
    /// Under the previous version, inside a rotation overlap only.
    pub previous: Option<D>,
}

impl<D: Copy> SecretDigests<D> {
    /// Every digest, the current one first.
    pub fn all(&self) -> impl Iterator<Item = D> {
        std::iter::once(self.current).chain(self.previous)
    }
}

/// Computes secret digests under the loaded secret versions: the only code
/// that reads a [`DeploymentSecret`]'s key, and the only maker of
/// [`CredentialHash`] and [`AccountHash`] outside tests.
///
/// Pure: the time that decides whether a rotation overlap is still open is
/// an argument, read by the caller from its [`crate::support::Clock`].
#[derive(Debug)]
pub struct KeyedHasher {
    current: DeploymentSecret,
    overlap: Option<Overlap>,
}

impl KeyedHasher {
    /// A hasher with one loaded version.
    pub fn new(current: DeploymentSecret) -> Self {
        Self {
            current,
            overlap: None,
        }
    }

    /// A hasher in a rotation: `current` is the new version, and `previous`
    /// keys digests too until `overlap_ends` (exclusive).
    pub fn rotating(
        current: DeploymentSecret,
        previous: DeploymentSecret,
        overlap_ends: Timestamp,
    ) -> Result<Self, InvalidRotation> {
        if previous.version >= current.version {
            return Err(InvalidRotation::NotOlder {
                previous: previous.version,
                current: current.version,
            });
        }
        Ok(Self {
            current,
            overlap: Some(Overlap {
                previous,
                ends: overlap_ends,
            }),
        })
    }

    pub const fn current_version(&self) -> SecretVersion {
        self.current.version
    }

    /// The previous version, while its overlap is open at `at`.
    pub fn previous_version(&self, at: Timestamp) -> Option<SecretVersion> {
        self.open_overlap(at)
            .map(|overlap| overlap.previous.version)
    }

    /// The digests of a raw credential (API key, OAuth access token,
    /// exchanged token, server key) at `at`.
    pub fn credential(&self, raw: &[u8], at: Timestamp) -> SecretDigests<CredentialHash> {
        self.digests(raw, at, CredentialHash::from_keyed_digest)
    }

    /// The digests of a raw account id at `at`.
    pub fn account(&self, raw: &[u8], at: Timestamp) -> SecretDigests<AccountHash> {
        self.digests(raw, at, AccountHash::from_keyed_digest)
    }

    fn open_overlap(&self, at: Timestamp) -> Option<&Overlap> {
        self.overlap.as_ref().filter(|overlap| at < overlap.ends)
    }

    fn digests<D>(
        &self,
        raw: &[u8],
        at: Timestamp,
        make: fn(SecretVersion, Blake3) -> D,
    ) -> SecretDigests<D> {
        let under = |secret: &DeploymentSecret| make(secret.version, secret.digest(raw));
        SecretDigests {
            current: under(&self.current),
            previous: self
                .open_overlap(at)
                .map(|overlap| under(&overlap.previous)),
        }
    }
}

/// The current version only; keys are never formatted.
impl fmt::Display for KeyedHasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "keyed hasher at secret version {}",
            self.current.version.0
        )
    }
}
