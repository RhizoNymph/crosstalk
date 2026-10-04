//! Credentials at the proxy: read, hashed at once, never kept.
//!
//! A raw credential is read from the request head as a [`RawCredential`], a
//! borrow of the header (or query) text that cannot outlive the request and
//! whose `Debug` prints a placeholder; it has no `Display`. It is hashed at
//! once by the [`KeyedHasher`] (BLAKE3 keyed with the current
//! [`DeploymentSecret`], and during a rotation overlap the previous one
//! too), and only the digests leave. A digest depends only on the secret and
//! the credential's own text, not on the header that carried it, the
//! scheme, the node or the time
//! (`ingress.credential.hash-depends-only-on-credential`).
//!
//! The hasher and the secret live in [`crate::ids`], the module that is to
//! be replaced by the spec's own (P0.7); they are re-exported here.
//! [`SecretError`] names environment variables, never their values.

use std::fmt;

use crate::config::{SecretRef, SecretsConfig};
pub use crate::ids::{DeploymentSecret, KeyedHasher, SameVersion, SecretFormat};

/// A raw credential, borrowed from the request head for exactly as long as
/// it takes to hash it. Never stored, logged or published.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RawCredential<'a>(&'a str);

impl<'a> RawCredential<'a> {
    /// `None` for an empty credential.
    pub fn new(text: &'a str) -> Option<Self> {
        (!text.is_empty()).then_some(Self(text))
    }

    /// The credential's shape, for the scheme rule. Never its content.
    pub(crate) fn shape(self) -> TokenShape {
        let text = self.0;
        if text.starts_with("sk-ant-oat") {
            TokenShape::AnthropicOauth
        } else if text.starts_with("sk-") {
            TokenShape::ApiKey
        } else if text.starts_with("eyJ") && text.split('.').count() == 3 {
            TokenShape::Jwt
        } else if text.starts_with("tid=") {
            TokenShape::CopilotMinted
        } else {
            TokenShape::Opaque
        }
    }

    /// The bytes the keyed hasher digests.
    pub(crate) fn bytes(self) -> &'a [u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for RawCredential<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RawCredential(<redacted>)")
    }
}

/// What a credential looks like, which is all the scheme rule may read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenShape {
    /// `sk-ant-oat…`: a Claude subscription OAuth access token.
    AnthropicOauth,
    /// `sk-ant-api…` and other `sk-` keys.
    ApiKey,
    /// A JSON Web Token (ChatGPT and Google OAuth access tokens).
    Jwt,
    /// Copilot's minted `tid=…;exp=…` token.
    CopilotMinted,
    Opaque,
}

/// Why the secrets could not be loaded. Names the environment variable,
/// never its value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretError {
    #[error("environment variable {env} is not set")]
    Missing { env: String },
    #[error("environment variable {env} {format}")]
    Malformed { env: String, format: SecretFormat },
    #[error(transparent)]
    SameVersion(#[from] SameVersion),
}

/// Load the secrets `config` names, reading each variable through `lookup`
/// (`|name| std::env::var(name).ok()` in production).
pub fn load_secrets(
    config: &SecretsConfig,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<KeyedHasher, SecretError> {
    let read = |secret: &SecretRef| -> Result<_, SecretError> {
        let text = lookup(&secret.env).ok_or_else(|| SecretError::Missing {
            env: secret.env.clone(),
        })?;
        let key = DeploymentSecret::from_hex(&text).map_err(|format| SecretError::Malformed {
            env: secret.env.clone(),
            format,
        })?;
        Ok((secret.version, key))
    };
    let current = read(&config.current)?;
    let previous = config.previous.as_ref().map(read).transpose()?;
    Ok(KeyedHasher::new(current, previous)?)
}
