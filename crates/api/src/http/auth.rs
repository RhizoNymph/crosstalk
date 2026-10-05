//! Who is calling: the request's credential headers, verified, then the
//! operator directory's [`Caller`] or the 401 to answer
//! ([`crosstalk_spec::interfaces::l8_surface::http::auth`]).
//!
//! ```text
//! Authorization (every field), Cookie (every field, joined with "; ")
//!   ─▶ CredentialHeaders ─▶ Verification::of(.., CredentialVerifier::verify)
//!   ─▶ authenticate(current OperatorDirectory) ─▶ Caller | AuthError (401)
//! ```
//!
//! Nothing else in a request reaches this: not the path, the query, the
//! body or any other header (`surface.api.caller-from-session`). The
//! directory is the latest one the gateway published on a `watch` channel,
//! so a config load takes effect for the next request.
//!
//! The deployment's API token (`api.token`, read from the environment
//! variable it names) is a [`BearerToken`], verified by [`StaticTokens`]
//! as the operator it is configured for. Session cookies are read and
//! verified by whatever [`CredentialVerifier`] the gateway supplies;
//! [`StaticTokens`] knows no sessions.

use std::fmt;
use std::sync::Arc;

use axum::http::HeaderMap;
use axum::http::header::{AUTHORIZATION, COOKIE};
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::Caller;
use crosstalk_spec::interfaces::l8_surface::http::auth::{
    AuthError, Credential, CredentialHeaders, Field, Verification, authenticate,
};
use crosstalk_spec::interfaces::l8_surface::operators::OperatorDirectory;
use crosstalk_spec::support::Blake3;
use tokio::sync::watch;

/// The session store's lookup: the operator a live credential belongs
/// to, or `None` for one it does not know or no longer accepts.
pub trait CredentialVerifier: Send + Sync + 'static {
    fn verify(&self, credential: &Credential) -> Option<OperatorId>;
}

/// Authentication for one server: the current operator directory and the
/// verifier.
#[derive(Clone)]
pub struct Auth {
    directory: watch::Receiver<OperatorDirectory>,
    verifier: Arc<dyn CredentialVerifier>,
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth")
            .field("mode", &self.directory.borrow().mode())
            .finish_non_exhaustive()
    }
}

impl Auth {
    /// The directory is whatever `directory` holds when a request
    /// arrives.
    pub fn new(
        directory: watch::Receiver<OperatorDirectory>,
        verifier: impl CredentialVerifier,
    ) -> Self {
        Self {
            directory,
            verifier: Arc::new(verifier),
        }
    }

    /// A directory that never changes.
    pub fn fixed(directory: OperatorDirectory, verifier: impl CredentialVerifier) -> Self {
        let (_, receiver) = watch::channel(directory);
        Self::new(receiver, verifier)
    }

    /// The request's caller, from its credential headers alone.
    pub(crate) fn caller(&self, headers: &HeaderMap) -> Result<Caller, AuthError> {
        let verification = verification(headers, self.verifier.as_ref());
        authenticate(&self.directory.borrow(), verification)
    }
}

fn verification(headers: &HeaderMap, verifier: &dyn CredentialVerifier) -> Verification {
    let mut fields = headers.get_all(AUTHORIZATION).iter();
    let authorization = match (fields.next(), fields.next()) {
        (None, _) => Field::Absent,
        (Some(_), Some(_)) => Field::Repeated,
        (Some(value), None) => match value.to_str() {
            Ok(text) => Field::One(text),
            // Not visible ASCII, so not `Bearer <b64token>`: malformed.
            Err(_) => return Verification::Rejected,
        },
    };
    // HTTP/2 may split the cookie across fields; RFC 9113 joins them with
    // "; ". A byte outside UTF-8 can only make the session cookie's value
    // fail `b64token`, which is what it should do.
    let cookies: Vec<String> = headers
        .get_all(COOKIE)
        .iter()
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .collect();
    let cookie = (!cookies.is_empty()).then(|| cookies.join("; "));
    let credential = CredentialHeaders {
        authorization,
        cookie: cookie.as_deref(),
    };
    Verification::of(&credential, |credential| verifier.verify(credential))
}

/// A bearer token the deployment accepts: RFC 6750 `b64token` text of at
/// least [`BearerToken::MIN_LEN`] characters. Its `Debug` hides it.
#[derive(Clone, PartialEq, Eq)]
pub struct BearerToken(String);

/// Why text is not a [`BearerToken`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBearerToken {
    #[error("the API token is shorter than {min} characters")]
    TooShort { min: usize },
    #[error("the API token is not b64token text (letters, digits, -._~+/ then any =)")]
    NotB64Token,
    #[error("the environment variable {name} is not set or not UTF-8")]
    Unset { name: String },
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(<redacted>)")
    }
}

impl BearerToken {
    /// Short enough to type, long enough not to guess.
    pub const MIN_LEN: usize = 16;

    pub fn new(text: &str) -> Result<Self, InvalidBearerToken> {
        if text.len() < Self::MIN_LEN {
            return Err(InvalidBearerToken::TooShort { min: Self::MIN_LEN });
        }
        let body = text.trim_end_matches('=');
        let b64token = !body.is_empty()
            && body.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
            });
        if !b64token {
            return Err(InvalidBearerToken::NotB64Token);
        }
        Ok(Self(text.to_owned()))
    }

    /// The token in the environment variable `name` (`api.token.env`).
    pub fn from_env(name: &str) -> Result<Self, InvalidBearerToken> {
        let text = std::env::var(name).map_err(|_| InvalidBearerToken::Unset {
            name: name.to_owned(),
        })?;
        Self::new(&text)
    }
}

/// Fixed bearer tokens, each verified as one operator. Tokens are kept as
/// BLAKE3 digests and every one is compared, in constant time, so neither
/// the table nor the comparison's timing holds a token. Sessions are not
/// known.
#[derive(Clone, Default)]
pub struct StaticTokens {
    tokens: Vec<([u8; 32], OperatorId)>,
}

impl fmt::Debug for StaticTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StaticTokens")
            .field("tokens", &self.tokens.len())
            .finish()
    }
}

impl StaticTokens {
    pub fn new(tokens: impl IntoIterator<Item = (BearerToken, OperatorId)>) -> Self {
        Self {
            tokens: tokens
                .into_iter()
                .map(|(token, operator)| (*Blake3::of(token.0.as_bytes()).as_bytes(), operator))
                .collect(),
        }
    }
}

impl CredentialVerifier for StaticTokens {
    fn verify(&self, credential: &Credential) -> Option<OperatorId> {
        let Credential::Bearer(secret) = credential else {
            return None;
        };
        let presented = *Blake3::of(secret.expose().as_bytes()).as_bytes();
        let mut found = None;
        for (digest, operator) in &self.tokens {
            let difference = digest
                .iter()
                .zip(presented.iter())
                .fold(0_u8, |acc, (a, b)| acc | (a ^ b));
            if difference == 0 && found.is_none() {
                found = Some(*operator);
            }
        }
        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_is_long_b64token_text() {
        assert_eq!(
            BearerToken::new("short"),
            Err(InvalidBearerToken::TooShort {
                min: BearerToken::MIN_LEN
            })
        );
        assert_eq!(
            BearerToken::new("has spaces in it, sadly"),
            Err(InvalidBearerToken::NotB64Token)
        );
        assert_eq!(
            BearerToken::new("================"),
            Err(InvalidBearerToken::NotB64Token)
        );
        assert!(BearerToken::new("abcdefghijklmnop-._~+/==").is_ok());
        assert_eq!(
            format!("{:?}", BearerToken::new("abcdefghijklmnop").ok()),
            "Some(BearerToken(<redacted>))"
        );
    }
}
