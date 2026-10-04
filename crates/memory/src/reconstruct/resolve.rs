//! The reference lookup behind `IdentityResolver::resolve`, and a reference
//! derivation of the evidence an exchange's client context carries.
//!
//! Resolution decides over the stored agents by the rules the spec states
//! (`IdentityResolver::resolve`):
//!
//! - the most specific evidence present decides (`IdentityEvidence::
//!   specificity`); less specific evidence never makes agents conflict;
//! - harness ids count only within their `IdentityScope`, which is part of
//!   the evidence value, so equal ids under two scopes never meet;
//! - a session id resolves only to the session's main agent: an agent in
//!   that scope holding the session and no `HarnessAgent` evidence;
//! - labels and harness claims are never read.
//!
//! Deriving the evidence is P4.1's `EvidenceDeriver`. [`context_evidence`]
//! is the part of it the tests here need: the evidence of an exchange's
//! `ClientContext` only. A `PromptFingerprint` needs a content hash of the
//! system prompt and first user turn, which this crate does not compute.

use std::collections::BTreeSet;

use crosstalk_spec::ids::AgentId;
use crosstalk_spec::interfaces::l3_reconstruction::Resolution;
use crosstalk_spec::observed::agent::{IdentityEvidence, IdentityScope};
use crosstalk_spec::observed::client::{ClientContext, Stability};
use crosstalk_spec::support::NonEmpty;

use super::table::AgentTable;

/// The scopes harness ids from `client` count in: the exchange's account if
/// it has one, else its credential if that is stable, else its upstream;
/// and during a secret rotation overlap also that scope under the previous
/// digest, so harness ids stored before the rotation still match.
fn scopes(client: &ClientContext) -> Vec<IdentityScope> {
    let stable_credential = client
        .credential
        .filter(|credential| matches!(credential.scheme.stability(), Stability::Stable))
        .map(|credential| credential.hash);
    let (current, older) = match (client.account, stable_credential) {
        (Some(account), _) => (
            IdentityScope::Account(account),
            client
                .previous_digests
                .and_then(|previous| previous.account)
                .map(IdentityScope::Account),
        ),
        (None, Some(credential)) => (
            IdentityScope::Credential(credential),
            client
                .previous_digests
                .and_then(|previous| previous.credential)
                .map(IdentityScope::Credential),
        ),
        (None, None) => (IdentityScope::Upstream(client.upstream.id.clone()), None),
    };
    std::iter::once(current).chain(older).collect()
}

/// The identity evidence `client` carries, most specific first.
pub fn context_evidence(client: &ClientContext) -> Vec<IdentityEvidence> {
    let mut evidence = Vec::new();
    for scope in scopes(client) {
        if let Some(agent) = &client.ids.agent {
            evidence.push(IdentityEvidence::HarnessAgent {
                scope: scope.clone(),
                agent: agent.clone(),
            });
        }
        if let Some(session) = &client.ids.session {
            evidence.push(IdentityEvidence::HarnessSession {
                scope,
                session: session.clone(),
            });
        }
    }
    let previous = client.previous_digests;
    for account in client
        .account
        .into_iter()
        .chain(previous.and_then(|p| p.account))
    {
        evidence.push(IdentityEvidence::Account(account));
    }
    if let Some(credential) = client.credential {
        let hashes = std::iter::once(credential.hash).chain(previous.and_then(|p| p.credential));
        for hash in hashes {
            match credential.scheme.stability() {
                Stability::Stable => evidence.push(IdentityEvidence::StableCredential(hash)),
                Stability::Rotating => evidence.push(IdentityEvidence::RotatingCredential(hash)),
                Stability::Shared => {}
            }
        }
    }
    evidence.sort_by_key(|item| std::cmp::Reverse(item.specificity()));
    evidence
}

/// Resolve `evidence` against the stored agents: `New` when no agent holds
/// its most specific evidence, `Known` (the canonical agent, with the
/// evidence it does not hold yet) when the holders form one cluster, and
/// `Conflict` (the clusters' canonical agents, ascending, and the deciding
/// evidence) otherwise.
pub(crate) fn resolve_evidence(
    table: &AgentTable,
    evidence: &NonEmpty<IdentityEvidence>,
) -> Resolution {
    let top = evidence
        .iter()
        .map(IdentityEvidence::specificity)
        .max()
        .unwrap_or_else(|| evidence.first().specificity());
    let deciding: Vec<&IdentityEvidence> = evidence
        .iter()
        .filter(|item| item.specificity() == top)
        .collect();
    let holds_harness_agent = |agent: &crosstalk_spec::observed::agent::Agent| {
        agent
            .evidence
            .iter()
            .any(|held| matches!(held, IdentityEvidence::HarnessAgent { .. }))
    };
    let holders: BTreeSet<AgentId> = table
        .agents
        .values()
        .filter(|agent| {
            deciding.iter().any(|item| {
                let held = agent.evidence.iter().any(|held| held == *item);
                match item {
                    IdentityEvidence::HarnessSession { .. } => held && !holds_harness_agent(agent),
                    IdentityEvidence::HarnessAgent { .. }
                    | IdentityEvidence::Account(_)
                    | IdentityEvidence::StableCredential(_)
                    | IdentityEvidence::RotatingCredential(_)
                    | IdentityEvidence::PromptFingerprint(_) => held,
                }
            })
        })
        .map(|agent| table.canonical(agent.id))
        .collect();
    let mut canonical = holders.into_iter();
    match (canonical.next(), canonical.next()) {
        (None, _) => Resolution::New {
            evidence: evidence.clone(),
        },
        (Some(agent), None) => {
            let held = table.agents.get(&agent);
            let new_evidence = evidence
                .iter()
                .filter(|item| held.is_none_or(|held| !held.evidence.iter().any(|h| h == *item)))
                .cloned()
                .collect();
            Resolution::Known {
                agent,
                new_evidence,
            }
        }
        (Some(first), Some(second)) => {
            let mut candidates = NonEmpty::new(first);
            candidates.push(second);
            for more in canonical {
                candidates.push(more);
            }
            let mut deciding = deciding.into_iter().cloned();
            let mut evidence =
                NonEmpty::new(deciding.next().unwrap_or_else(|| evidence.first().clone()));
            for item in deciding {
                evidence.push(item);
            }
            Resolution::Conflict {
                candidates,
                evidence,
            }
        }
    }
}
