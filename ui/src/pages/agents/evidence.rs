//! Identity evidence in words, and how two agents' evidence compares.
//!
//! Hashes are abbreviated; harness ids are shown, since they are what an
//! operator matches against a harness's logs.

use crosstalk_spec::ids::{AccountHash, CredentialHash};
use crosstalk_spec::observed::agent::{IdentityEvidence, IdentityScope, Strength};

use crate::components::abbrev_digest;

/// Harness ids longer than this are cut in the middle of the display.
const ID_CHARS: usize = 24;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceRow {
    pub kind: &'static str,
    /// The value shown, abbreviated.
    pub value: String,
    /// The full value where the shown one is cut, for a tooltip.
    pub full: String,
    pub scope: Option<String>,
    pub strength: Strength,
    pub specificity: u8,
    /// Also held by the other agent of a merge comparison.
    pub shared: bool,
}

fn keyed(version: u16, digest: String) -> String {
    format!("k{version}:{digest}")
}

pub fn account_text(hash: &AccountHash) -> String {
    keyed(hash.key().0, abbrev_digest(hash.digest()))
}

pub fn credential_text(hash: &CredentialHash) -> String {
    keyed(hash.key().0, abbrev_digest(hash.digest()))
}

pub fn scope_text(scope: &IdentityScope) -> String {
    match scope {
        IdentityScope::Account(hash) => format!("account {}", account_text(hash)),
        IdentityScope::Credential(hash) => format!("credential {}", credential_text(hash)),
        IdentityScope::Upstream(upstream) => format!("upstream {}", upstream.0),
    }
}

fn cut(id: &str) -> String {
    let count = id.chars().count();
    if count <= ID_CHARS {
        return id.to_owned();
    }
    let head: String = id.chars().take(ID_CHARS / 2).collect();
    let tail: String = id.chars().skip(count - ID_CHARS / 4).collect();
    format!("{head}…{tail}")
}

pub fn evidence_row(evidence: &IdentityEvidence) -> EvidenceRow {
    let (kind, full, scope) = match evidence {
        IdentityEvidence::HarnessAgent { scope, agent } => {
            ("harness agent id", agent.clone(), Some(scope_text(scope)))
        }
        IdentityEvidence::HarnessSession { scope, session } => (
            "harness session id",
            session.clone(),
            Some(scope_text(scope)),
        ),
        IdentityEvidence::Account(hash) => ("account", account_text(hash), None),
        IdentityEvidence::StableCredential(hash) => {
            ("stable credential", credential_text(hash), None)
        }
        IdentityEvidence::RotatingCredential(hash) => {
            ("rotating credential", credential_text(hash), None)
        }
        IdentityEvidence::PromptFingerprint(hash) => {
            ("prompt fingerprint", abbrev_digest(hash.digest()), None)
        }
    };
    EvidenceRow {
        kind,
        value: cut(&full),
        full,
        scope,
        strength: evidence.strength(),
        specificity: evidence.specificity(),
        shared: false,
    }
}

/// Evidence rows, most specific first, each marked shared when `other`
/// holds the same evidence.
pub fn evidence_rows<'a>(
    evidence: impl Iterator<Item = &'a IdentityEvidence>,
    other: &[&IdentityEvidence],
) -> Vec<EvidenceRow> {
    let mut rows: Vec<(u8, EvidenceRow)> = evidence
        .map(|e| {
            let mut row = evidence_row(e);
            row.shared = other.contains(&e);
            (e.specificity(), row)
        })
        .collect();
    rows.sort_by(|a, b| b.0.cmp(&a.0));
    rows.into_iter().map(|(_, row)| row).collect()
}

/// Reasons two agents look like different agents: harness ids of the same
/// kind in the same scope with different values. The resolver would never
/// have merged them on those.
pub fn conflicts(a: &[&IdentityEvidence], b: &[&IdentityEvidence]) -> Vec<String> {
    let mut out = Vec::new();
    for x in a {
        for y in b {
            let differs = match (x, y) {
                (
                    IdentityEvidence::HarnessAgent {
                        scope: s1,
                        agent: v1,
                    },
                    IdentityEvidence::HarnessAgent {
                        scope: s2,
                        agent: v2,
                    },
                ) if s1 == s2 && v1 != v2 => Some(("harness agent ids", s1)),
                (
                    IdentityEvidence::HarnessSession {
                        scope: s1,
                        session: v1,
                    },
                    IdentityEvidence::HarnessSession {
                        scope: s2,
                        session: v2,
                    },
                ) if s1 == s2 && v1 != v2 => Some(("harness session ids", s1)),
                _ => None,
            };
            if let Some((what, scope)) = differs {
                let line = format!(
                    "Different {what} in the same scope ({}).",
                    scope_text(scope)
                );
                if !out.contains(&line) {
                    out.push(line);
                }
            }
        }
    }
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use crosstalk_spec::ids::{PromptHash, SecretVersion};
    use crosstalk_spec::observed::client::UpstreamId;
    use crosstalk_spec::support::Blake3;

    use super::*;

    pub fn digest(first: u8) -> Blake3 {
        let mut bytes = [0u8; 32];
        bytes[0] = first;
        Blake3::from_bytes(bytes)
    }

    pub fn harness(agent: &str) -> IdentityEvidence {
        IdentityEvidence::HarnessAgent {
            scope: IdentityScope::Upstream(UpstreamId("vllm".into())),
            agent: agent.into(),
        }
    }

    pub fn credential(first: u8) -> IdentityEvidence {
        IdentityEvidence::StableCredential(CredentialHash::from_keyed_digest(
            SecretVersion(1),
            digest(first),
        ))
    }

    #[test]
    fn rows_name_kind_scope_and_strength() {
        let row = evidence_row(&harness("agent-7"));
        assert_eq!(row.kind, "harness agent id");
        assert_eq!(row.value, "agent-7");
        assert_eq!(row.scope.as_deref(), Some("upstream vllm"));
        assert_eq!(row.strength, Strength::Strong);
        let row = evidence_row(&credential(0xab));
        assert_eq!(row.value, "k1:ab000000…");
        let row = evidence_row(&IdentityEvidence::PromptFingerprint(
            PromptHash::from_digest(digest(1)),
        ));
        assert_eq!(row.strength, Strength::Weak);
        assert_eq!(row.value, "01000000…");
    }

    #[test]
    fn long_harness_ids_are_cut_but_kept_whole_for_tooltips() {
        let id = "0123456789abcdef0123456789abcdef";
        let row = evidence_row(&harness(id));
        assert_eq!(row.value, "0123456789ab…abcdef");
        assert_eq!(row.full, id);
    }

    #[test]
    fn rows_sort_by_specificity_and_mark_shared() {
        let shared = credential(1);
        let a = [credential(2), harness("x"), shared.clone()];
        let rows = evidence_rows(a.iter(), &[&shared]);
        assert_eq!(rows[0].kind, "harness agent id");
        let shared_rows: Vec<_> = rows.iter().filter(|r| r.shared).collect();
        assert_eq!(shared_rows.len(), 1);
        assert_eq!(shared_rows[0].value, "k1:01000000…");
    }

    #[test]
    fn different_harness_ids_in_one_scope_conflict() {
        let a = harness("one");
        let b = harness("two");
        let lines = conflicts(&[&a], &[&b]);
        assert_eq!(
            lines,
            ["Different harness agent ids in the same scope (upstream vllm)."]
        );
        assert!(conflicts(&[&a], &[&a]).is_empty());
        assert!(conflicts(&[&credential(1)], &[&credential(2)]).is_empty());
    }
}
