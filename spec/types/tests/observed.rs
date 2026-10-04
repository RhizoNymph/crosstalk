use crate::ids::{AccountHash, CredentialHash, PromptHash, SecretVersion};
use crate::observed::agent::{
    IdentityEvidence, IdentityScope, MergeAuthor, MergeRequest, SelfMerge, Strength,
};
use crate::observed::client::{
    CredentialScheme, Dialect, InferenceServer, Stability, UpstreamKind, Vendor,
};
use crate::support::Blake3;
use crate::tests::fixtures::agent;

fn credential(byte: u8) -> CredentialHash {
    CredentialHash::from_keyed_digest(SecretVersion(1), Blake3::from_bytes([byte; 32]))
}

fn all_evidence() -> Vec<IdentityEvidence> {
    let scope = IdentityScope::Credential(credential(1));
    vec![
        IdentityEvidence::HarnessAgent {
            scope: scope.clone(),
            agent: "a".into(),
        },
        IdentityEvidence::HarnessSession {
            scope,
            session: "s".into(),
        },
        IdentityEvidence::Account(AccountHash::from_keyed_digest(
            SecretVersion(1),
            Blake3::from_bytes([2; 32]),
        )),
        IdentityEvidence::StableCredential(credential(3)),
        IdentityEvidence::PromptFingerprint(PromptHash::from_digest(Blake3::from_bytes([4; 32]))),
        IdentityEvidence::RotatingCredential(credential(5)),
    ]
}

#[test]
fn evidence_specificity_is_a_strict_order() {
    let ranks: Vec<u8> = all_evidence()
        .iter()
        .map(IdentityEvidence::specificity)
        .collect();
    assert!(ranks.windows(2).all(|w| w[0] > w[1]), "{ranks:?}");
}

#[test]
fn rotating_credentials_and_prompts_are_weak() {
    let strengths: Vec<Strength> = all_evidence()
        .iter()
        .map(IdentityEvidence::strength)
        .collect();
    assert_eq!(
        strengths,
        vec![
            Strength::Strong,
            Strength::Strong,
            Strength::Strong,
            Strength::Strong,
            Strength::Weak,
            Strength::Weak,
        ]
    );
}

#[test]
fn merge_request_rejects_self_merge() {
    assert_eq!(
        MergeRequest::new(agent(1), agent(1), MergeAuthor::Resolver),
        Err(SelfMerge)
    );
    let request =
        MergeRequest::new(agent(1), agent(2), MergeAuthor::Resolver).expect("different agents");
    assert_eq!((request.source(), request.target()), (agent(1), agent(2)));
}

#[test]
fn credential_stability_follows_scheme() {
    assert_eq!(CredentialScheme::ApiKey.stability(), Stability::Stable);
    assert_eq!(
        CredentialScheme::OAuthAccessToken.stability(),
        Stability::Rotating
    );
    assert_eq!(
        CredentialScheme::ExchangedToken.stability(),
        Stability::Rotating
    );
    assert_eq!(CredentialScheme::ServerKey.stability(), Stability::Shared);
}

#[test]
fn dialect_follows_upstream() {
    let cases = [
        (
            UpstreamKind::VendorApi(Vendor::Anthropic),
            Dialect::Reference,
        ),
        (
            UpstreamKind::Subscription(Vendor::OpenAi),
            Dialect::Reference,
        ),
        (
            UpstreamKind::Subscription(Vendor::GithubCopilot),
            Dialect::Copilot,
        ),
        (
            UpstreamKind::InferenceServer(InferenceServer::Vllm),
            Dialect::Vllm,
        ),
        (
            UpstreamKind::InferenceServer(InferenceServer::Sglang),
            Dialect::Sglang,
        ),
    ];
    for (kind, dialect) in cases {
        assert_eq!(kind.dialect(), dialect, "{kind:?}");
    }
}

#[test]
fn secret_digests_remember_their_key_version() {
    let old = CredentialHash::from_keyed_digest(SecretVersion(1), Blake3::from_bytes([9; 32]));
    let new = CredentialHash::from_keyed_digest(SecretVersion(2), Blake3::from_bytes([9; 32]));
    assert_eq!(old.key(), SecretVersion(1));
    assert_ne!(old, new);
}
