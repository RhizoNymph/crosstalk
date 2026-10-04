//! Where a caller's identity is interpreted: the one place that decides an
//! exchange's [`IdentityScope`] and its caller evidence (credential and
//! account).
//!
//! Everything that scopes identity reads it here: the harness ids
//! [`super::HeaderEvidence`] derives, the credential and account evidence
//! [`super::ApiKeyEvidence`] derives, and the scope a stored response is
//! filed under for WebSocket increment resolution
//! (`reconstruct.thread.previous-response-scoped`). A new scoping dimension
//! (a replay corpus, so an offline dataset's credentials never meet live
//! ones) is added to [`CallerScope`] and the two functions below, and every
//! caller follows.

use crosstalk_spec::observed::agent::{IdentityEvidence, IdentityScope};
use crosstalk_spec::observed::client::{ClientContext, Stability};

/// The scopes harness ids from one exchange count in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerScope {
    /// The exchange's account if it has one, else its credential when that
    /// is stable, else its upstream (`reconstruct.evidence.scope-precedence`).
    pub current: IdentityScope,
    /// The same scope under the previous secret version, during a rotation
    /// overlap: harness ids stored before the rotation still match.
    pub previous: Option<IdentityScope>,
}

impl CallerScope {
    /// Every scope, current first.
    pub fn all(&self) -> impl Iterator<Item = &IdentityScope> {
        std::iter::once(&self.current).chain(self.previous.as_ref())
    }
}

/// The scopes of `client`'s harness ids. Rotating, shared and missing
/// credentials never scope: a token refresh keeps the scope, and with it
/// the agent.
pub fn scope_of(client: &ClientContext) -> CallerScope {
    let stable_credential = client
        .credential
        .filter(|credential| matches!(credential.scheme.stability(), Stability::Stable));
    let previous = client.previous_digests;
    match (client.account, stable_credential) {
        (Some(account), _) => CallerScope {
            current: IdentityScope::Account(account),
            previous: previous
                .and_then(|previous| previous.account)
                .map(IdentityScope::Account),
        },
        (None, Some(credential)) => CallerScope {
            current: IdentityScope::Credential(credential.hash),
            previous: previous
                .and_then(|previous| previous.credential)
                .map(IdentityScope::Credential),
        },
        (None, None) => CallerScope {
            current: IdentityScope::Upstream(client.upstream.id.clone()),
            previous: None,
        },
    }
}

/// The caller evidence `client` carries: its account, then its credential
/// by stability (`reconstruct.evidence.credential-follows-stability`: a
/// stable one is `StableCredential`, a rotating one `RotatingCredential`, a
/// shared or missing one nothing), each also under the previous secret
/// version during a rotation overlap.
pub fn caller_evidence(client: &ClientContext) -> Vec<IdentityEvidence> {
    let previous = client.previous_digests;
    let mut evidence: Vec<IdentityEvidence> = client
        .account
        .into_iter()
        .chain(previous.and_then(|previous| previous.account))
        .map(IdentityEvidence::Account)
        .collect();
    if let Some(credential) = client.credential {
        let hashes = std::iter::once(credential.hash)
            .chain(previous.and_then(|previous| previous.credential));
        for hash in hashes {
            match credential.scheme.stability() {
                Stability::Stable => evidence.push(IdentityEvidence::StableCredential(hash)),
                Stability::Rotating => evidence.push(IdentityEvidence::RotatingCredential(hash)),
                Stability::Shared => {}
            }
        }
    }
    evidence
}
