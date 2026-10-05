//! Identity evidence: what an exchange says about who sent it.
//!
//! The spec's [`EvidenceDeriver`] is a computation over an exchange's
//! metadata and request; it reads no store. This module implements it in
//! parts, each one evidence source:
//!
//! - [`ApiKeyEvidence`]: the account and the credential by its stability
//!   ([`scope::caller_evidence`]);
//! - [`HeaderEvidence`]: the harness agent and session ids, each scoped by
//!   the exchange's [`IdentityScope`] ([`scope::scope_of`]);
//! - [`PromptFingerprintEvidence`]: the system prompt plus first user turn;
//! - [`ChainEvidence`]: all of them, most specific first. The deriver the
//!   reconstruct consumer uses.
//!
//! Harness claims (family, version, User-Agent) are never read here: no
//! `IdentityEvidence` variant can carry one
//! (`reconstruct.evidence.harness-claim-never-evidence`), and the claim is
//! recorded in the claim store instead.

pub mod scope;

use crosstalk_spec::ids::PromptHash;
use crosstalk_spec::interfaces::l3_reconstruction::EvidenceDeriver;
use crosstalk_spec::observed::agent::{IdentityEvidence, IdentityScope};
use crosstalk_spec::observed::exchange::ExchangeMeta;
use crosstalk_spec::observed::message::{Message, MessageBody};
use crosstalk_spec::support::Blake3;

pub use scope::{CallerScope, caller_evidence, scope_of};

/// The account and the credential, by stability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApiKeyEvidence;

impl EvidenceDeriver for ApiKeyEvidence {
    fn derive(&self, meta: &ExchangeMeta, _request: &[Message]) -> Vec<IdentityEvidence> {
        caller_evidence(&meta.client)
    }
}

/// The harness agent and session ids, scoped: equal ids under two scopes
/// are different evidence (`reconstruct.resolve.harness-ids-scoped`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeaderEvidence;

impl EvidenceDeriver for HeaderEvidence {
    fn derive(&self, meta: &ExchangeMeta, _request: &[Message]) -> Vec<IdentityEvidence> {
        let ids = &meta.client.ids;
        let scope = scope_of(&meta.client);
        let mut evidence = Vec::new();
        for scope in scope.all() {
            if let Some(agent) = &ids.agent {
                evidence.push(IdentityEvidence::HarnessAgent {
                    scope: scope.clone(),
                    agent: agent.clone(),
                });
            }
            if let Some(session) = &ids.session {
                evidence.push(IdentityEvidence::HarnessSession {
                    scope: scope.clone(),
                    session: session.clone(),
                });
            }
        }
        evidence
    }
}

/// The harness agent id of `meta`'s parent, in each of the exchange's
/// scopes: what a sub-agent's parent holds. Empty when the harness sent no
/// parent id.
pub fn parent_agent_evidence(meta: &ExchangeMeta) -> Vec<IdentityEvidence> {
    let Some(parent) = &meta.client.ids.parent_agent else {
        return Vec::new();
    };
    scope_of(&meta.client)
        .all()
        .map(|scope| IdentityEvidence::HarnessAgent {
            scope: scope.clone(),
            agent: parent.clone(),
        })
        .collect()
}

/// The harness session of `meta`, in each of the exchange's scopes: what
/// the session's main agent holds. Empty when the harness sent no session.
pub fn session_evidence(meta: &ExchangeMeta) -> Vec<IdentityEvidence> {
    let Some(session) = &meta.client.ids.session else {
        return Vec::new();
    };
    scope_of(&meta.client)
        .all()
        .map(|scope: &IdentityScope| IdentityEvidence::HarnessSession {
            scope: scope.clone(),
            session: session.clone(),
        })
        .collect()
}

/// The domain separator of the prompt fingerprint's digest.
const FINGERPRINT_DOMAIN: &[u8] = b"crosstalk.reconstruct.prompt-fingerprint.v1";

/// The system prompt plus the first user turn, as a [`PromptHash`]: the
/// BLAKE3 of a domain separator, every system message before the first
/// user message (in order) and that user message, by message hash. Weak
/// evidence: agents that share a credential and a prompt share it. None
/// for a request with no user message.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PromptFingerprintEvidence;

impl PromptFingerprintEvidence {
    /// The fingerprint of `request`, if it has a user message.
    pub fn fingerprint(request: &[Message]) -> Option<PromptHash> {
        let first_user = request
            .iter()
            .position(|message| matches!(message.body, MessageBody::User(_)))?;
        let mut bytes = FINGERPRINT_DOMAIN.to_vec();
        for message in &request[..first_user] {
            if matches!(message.body, MessageBody::System(_)) {
                bytes.push(b's');
                bytes.extend_from_slice(message.hash.digest().as_bytes());
            }
        }
        bytes.push(b'u');
        bytes.extend_from_slice(request[first_user].hash.digest().as_bytes());
        Some(PromptHash::from_digest(Blake3::of(&bytes)))
    }
}

impl EvidenceDeriver for PromptFingerprintEvidence {
    fn derive(&self, _meta: &ExchangeMeta, request: &[Message]) -> Vec<IdentityEvidence> {
        Self::fingerprint(request)
            .map(IdentityEvidence::PromptFingerprint)
            .into_iter()
            .collect()
    }
}

/// Every evidence source above, concatenated and then ordered most
/// specific first (`IdentityEvidence::specificity` descending; equal
/// specificity keeps derivation order), with repeats dropped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChainEvidence {
    /// Whether to derive the prompt fingerprint. On by default; a
    /// deployment whose agents share prompts may turn it off.
    pub without_fingerprint: bool,
}

impl EvidenceDeriver for ChainEvidence {
    fn derive(&self, meta: &ExchangeMeta, request: &[Message]) -> Vec<IdentityEvidence> {
        let mut evidence = HeaderEvidence.derive(meta, request);
        evidence.extend(ApiKeyEvidence.derive(meta, request));
        if !self.without_fingerprint {
            evidence.extend(PromptFingerprintEvidence.derive(meta, request));
        }
        let mut unique: Vec<IdentityEvidence> = Vec::with_capacity(evidence.len());
        for item in evidence {
            if !unique.contains(&item) {
                unique.push(item);
            }
        }
        unique.sort_by_key(|item| std::cmp::Reverse(item.specificity()));
        unique
    }
}
