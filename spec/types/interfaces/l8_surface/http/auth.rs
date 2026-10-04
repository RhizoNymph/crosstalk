//! Who is calling: a request's credential, what verifying it established
//! ([`Verification`], which becomes the directory's
//! [`RequestIdentity`]), and the request's [`Caller`] or its 401.
//!
//! ```text
//! Authorization, Cookie ─▶ CredentialHeaders::credential ─▶ Credential | none | malformed
//!   ─▶ (the session store verifies it) ─▶ Verification ─▶ authenticate(directory) ─▶ Caller | AuthError (401)
//! ```
//!
//! **Only the credential.** [`CredentialHeaders`] holds the two header
//! fields a credential travels in and nothing else, and [`authenticate`]
//! takes only what verifying it established, so no path, query parameter,
//! body or other header can name or change the caller.
//!
//! - **Bearer token:** `Authorization: Bearer <token>`, for clients that
//!   are not browsers (the UI server, scripts). The scheme is matched
//!   ignoring case, the token is `b64token` text (RFC 6750).
//! - **Session cookie:** [`SESSION_COOKIE`] (`__Host-crosstalk-session`),
//!   for browsers, whose `EventSource` cannot send an `Authorization`
//!   header. The `__Host-` prefix makes the browser keep it only when it
//!   was set `Secure`, with `Path=/` and no `Domain`; the surface also sets
//!   it `HttpOnly` and `SameSite=Strict`, so no other site's page sends it.
//!
//! When a request carries an `Authorization` header, that is its
//! credential and the cookie is not read. More than one `Authorization`
//! field, a scheme other than `Bearer`, a token that is not `b64token`,
//! or the cookie named twice is a malformed credential. How tokens and
//! sessions are issued, stored and verified is the session store's, outside
//! this binding.
//!
//! **Trusted mode** ignores credentials: every request is the trusted
//! operator's, as [`OperatorDirectory::caller`] defines, so the surface
//! need not read or verify one.
//!
//! **401 versus 403.** No credential, a credential that is malformed or
//! fails verification, or one naming an operator config does not define
//! (now or ever) is `401 Unauthorized` with an [`AuthError`] body and a
//! `WWW-Authenticate` header ([`AuthError::www_authenticate`]); the request
//! reaches no method and is not audited. A caller that is authenticated
//! but lacks the route's permission is `403 Forbidden` with
//! `QueryError::Forbidden`, which the method returns (and an action
//! audits).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::ids::OperatorId;

use super::super::Caller;
use super::super::operators::{OperatorDirectory, RequestIdentity, Unauthenticated};

/// The session cookie's name.
pub const SESSION_COOKIE: &str = "__Host-crosstalk-session";

/// `WWW-Authenticate`'s realm.
pub const REALM: &str = "crosstalk";

/// One request header field as received: absent, present once, or present
/// more than once (which no credential header may be).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field<'a> {
    Absent,
    One(&'a str),
    Repeated,
}

/// The header fields a credential travels in, and the only input to
/// authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialHeaders<'a> {
    pub authorization: Field<'a>,
    /// The `Cookie` field; HTTP/2's separate `cookie` fields joined with
    /// `"; "` first, as RFC 9113 requires.
    pub cookie: Option<&'a str>,
}

/// A secret: a bearer token or a session id. Its `Debug` hides it, so a
/// credential logged by mistake shows no secret
/// (`surface.api.no-raw-credentials`).
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// The secret itself, for the session store to verify.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// The credential a request presented.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Credential {
    Bearer(Secret),
    Session(Secret),
}

/// Why the credential headers hold no usable credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MalformedCredential {
    RepeatedAuthorization,
    /// An `Authorization` scheme other than `Bearer`, or no token after it.
    NotBearer,
    /// A token or cookie value outside `b64token` text.
    InvalidToken,
    /// The session cookie named more than once.
    RepeatedCookie,
}

impl CredentialHeaders<'_> {
    /// The credential the headers carry: the `Authorization` bearer token
    /// if there is that header, else the session cookie, else none.
    pub fn credential(&self) -> Result<Option<Credential>, MalformedCredential> {
        match self.authorization {
            Field::Repeated => Err(MalformedCredential::RepeatedAuthorization),
            Field::One(value) => bearer(value).map(|token| Some(Credential::Bearer(token))),
            Field::Absent => match self.cookie {
                None => Ok(None),
                Some(cookie) => session(cookie).map(|id| id.map(Credential::Session)),
            },
        }
    }
}

fn bearer(value: &str) -> Result<Secret, MalformedCredential> {
    let (scheme, token) = value
        .trim()
        .split_once(' ')
        .ok_or(MalformedCredential::NotBearer)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(MalformedCredential::NotBearer);
    }
    let token = token.trim_start_matches(' ');
    if token.is_empty() {
        return Err(MalformedCredential::NotBearer);
    }
    b64token(token)
}

fn session(cookie: &str) -> Result<Option<Secret>, MalformedCredential> {
    let mut found = None;
    for pair in cookie.split(';') {
        let Some((name, value)) = pair.trim().split_once('=') else {
            continue;
        };
        if name == SESSION_COOKIE {
            if found.is_some() {
                return Err(MalformedCredential::RepeatedCookie);
            }
            found = Some(b64token(value)?);
        }
    }
    Ok(found)
}

/// RFC 6750's `b64token`: `1*( ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/" ) *"="`.
fn b64token(text: &str) -> Result<Secret, MalformedCredential> {
    let body = text.trim_end_matches('=');
    let valid = !body.is_empty()
        && body.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        });
    if valid {
        Ok(Secret(text.to_owned()))
    } else {
        Err(MalformedCredential::InvalidToken)
    }
}

/// What verifying a request's credential established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    /// The request carried no credential.
    NoCredential,
    /// It carried one that was malformed, unknown, expired or revoked.
    Rejected,
    /// A credential the session store verified for this operator.
    Verified(OperatorId),
}

impl Verification {
    /// Verifies the headers' credential with `verify` (the session store's
    /// lookup: the operator a live token or session belongs to).
    pub fn of(
        headers: &CredentialHeaders<'_>,
        verify: impl FnOnce(&Credential) -> Option<OperatorId>,
    ) -> Self {
        match headers.credential() {
            Ok(None) => Self::NoCredential,
            Err(_) => Self::Rejected,
            Ok(Some(credential)) => verify(&credential).map_or(Self::Rejected, Self::Verified),
        }
    }

    /// The directory's view of it: verified or anonymous.
    pub fn identity(self) -> RequestIdentity {
        match self {
            Self::NoCredential | Self::Rejected => RequestIdentity::Anonymous,
            Self::Verified(operator) => RequestIdentity::Verified(operator),
        }
    }
}

/// The request's caller: `OperatorDirectory::caller` of the verification's
/// identity, or the 401 to answer.
pub fn authenticate(
    directory: &OperatorDirectory,
    verification: Verification,
) -> Result<Caller, AuthError> {
    directory
        .caller(verification.identity())
        .map_err(|refused| AuthError {
            reason: match (refused, verification) {
                (Unauthenticated::NoSession, Verification::NoCredential) => {
                    AuthFailure::NoCredential
                }
                (
                    Unauthenticated::NoSession,
                    Verification::Rejected | Verification::Verified(_),
                ) => AuthFailure::InvalidCredential,
                (Unauthenticated::UnknownOperator(_), _) => AuthFailure::UnknownOperator,
                (Unauthenticated::FormerOperator(_), _) => AuthFailure::FormerOperator,
            },
        })
}

/// The body of a `401 Unauthorized`: `{"reason": "invalid_credential"}`.
/// A response, never a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub struct AuthError {
    pub reason: AuthFailure,
}

/// Why a request has no caller. On the wire, a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthFailure {
    /// No `Authorization` header and no session cookie.
    NoCredential,
    /// A credential that is malformed, or that the session store does not
    /// know or no longer accepts: sign in again.
    InvalidCredential,
    /// A verified credential for an operator the directory never held.
    UnknownOperator,
    /// A verified credential for an operator config no longer defines.
    FormerOperator,
}

impl AuthError {
    /// The `WWW-Authenticate` header a 401 carries (RFC 9110, RFC 6750):
    /// the bearer challenge, with `error="invalid_token"` when a credential
    /// was presented.
    pub fn www_authenticate(&self) -> String {
        match self.reason {
            AuthFailure::NoCredential => format!("Bearer realm=\"{REALM}\""),
            AuthFailure::InvalidCredential
            | AuthFailure::UnknownOperator
            | AuthFailure::FormerOperator => {
                format!("Bearer realm=\"{REALM}\", error=\"invalid_token\"")
            }
        }
    }
}
