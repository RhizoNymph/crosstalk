//! Evidence derivation: credentials by stability, the scope harness ids
//! count in, the prompt fingerprint, and the chain's order.

use crosstalk_spec::interfaces::l3_reconstruction::EvidenceDeriver;
use crosstalk_spec::observed::agent::{IdentityEvidence, IdentityScope};
use crosstalk_spec::observed::client::{CredentialRef, CredentialScheme, PreviousDigests};
use crosstalk_spec::observed::exchange::ExchangeMeta;
use crosstalk_testkit::build::ExchangeBuilder;
use crosstalk_testkit::build::message::{message, system_text, user_text};
use crosstalk_testkit::ids::Ids;

use crate::evidence::{
    ApiKeyEvidence, ChainEvidence, HeaderEvidence, PromptFingerprintEvidence, scope_of,
};

fn meta(ids: &mut Ids, scheme: Option<CredentialScheme>) -> ExchangeMeta {
    let credential = scheme.map(|scheme| CredentialRef {
        scheme,
        hash: ids.credential(),
    });
    ExchangeBuilder::new(ids)
        .credential(credential)
        .build()
        .meta
}

fn credential_evidence(meta: &ExchangeMeta) -> Vec<IdentityEvidence> {
    ApiKeyEvidence.derive(meta, &[])
}

/// `reconstruct.evidence.credential-follows-stability`: an API key.
#[test]
fn api_key_yields_stable_credential() {
    let mut ids = Ids::new();
    let meta = meta(&mut ids, Some(CredentialScheme::ApiKey));
    let hash = meta.client.credential.map(|c| c.hash).expect("credential");
    assert_eq!(
        credential_evidence(&meta),
        vec![IdentityEvidence::StableCredential(hash)]
    );
}

/// `reconstruct.evidence.credential-follows-stability`: OAuth and
/// exchanged tokens.
#[test]
fn oauth_and_exchanged_tokens_yield_rotating_credential() {
    let mut ids = Ids::new();
    for scheme in [
        CredentialScheme::OauthAccessToken,
        CredentialScheme::ExchangedToken,
    ] {
        let meta = meta(&mut ids, Some(scheme));
        let hash = meta.client.credential.map(|c| c.hash).expect("credential");
        assert_eq!(
            credential_evidence(&meta),
            vec![IdentityEvidence::RotatingCredential(hash)]
        );
    }
}

/// `reconstruct.evidence.credential-follows-stability`: a server key and
/// no credential.
#[test]
fn server_key_and_no_credential_yield_no_credential_evidence() {
    let mut ids = Ids::new();
    for scheme in [Some(CredentialScheme::ServerKey), None] {
        let meta = meta(&mut ids, scheme);
        assert_eq!(credential_evidence(&meta), Vec::new());
    }
}

/// `reconstruct.evidence.scope-precedence`: an account scopes harness ids,
/// whatever the credential.
#[test]
fn account_scopes_harness_ids() {
    let mut ids = Ids::new();
    let mut meta = meta(&mut ids, Some(CredentialScheme::ApiKey));
    let account = ids.account();
    meta.client.account = Some(account);
    meta.client.ids.agent = Some("agent-1".to_owned());
    assert_eq!(
        scope_of(&meta.client).current,
        IdentityScope::Account(account)
    );
    assert!(
        HeaderEvidence
            .derive(&meta, &[])
            .iter()
            .all(|item| matches!(
                item,
                IdentityEvidence::HarnessAgent { scope: IdentityScope::Account(a), .. }
                    | IdentityEvidence::HarnessSession { scope: IdentityScope::Account(a), .. }
                    if *a == account
            ))
    );
}

/// `reconstruct.evidence.scope-precedence`: without an account, a stable
/// credential scopes them.
#[test]
fn stable_credential_scopes_harness_ids_without_account() {
    let mut ids = Ids::new();
    let meta = meta(&mut ids, Some(CredentialScheme::ApiKey));
    let hash = meta.client.credential.map(|c| c.hash).expect("credential");
    assert_eq!(
        scope_of(&meta.client).current,
        IdentityScope::Credential(hash)
    );
    let session = meta.client.ids.session.clone().expect("session");
    assert_eq!(
        HeaderEvidence.derive(&meta, &[]),
        vec![IdentityEvidence::HarnessSession {
            scope: IdentityScope::Credential(hash),
            session
        }]
    );
}

/// `reconstruct.evidence.scope-precedence`: a rotating, shared or missing
/// credential scopes them to the upstream.
#[test]
fn rotating_shared_or_no_credential_scopes_to_upstream() {
    let mut ids = Ids::new();
    for scheme in [
        Some(CredentialScheme::OauthAccessToken),
        Some(CredentialScheme::ExchangedToken),
        Some(CredentialScheme::ServerKey),
        None,
    ] {
        let meta = meta(&mut ids, scheme);
        assert_eq!(
            scope_of(&meta.client).current,
            IdentityScope::Upstream(meta.client.upstream.id.clone())
        );
        assert_eq!(scope_of(&meta.client).previous, None);
    }
}

/// During a rotation overlap the previous digests are evidence too, under
/// the previous scope.
#[test]
fn rotation_overlap_derives_both_digests() {
    let mut ids = Ids::new();
    let mut meta = meta(&mut ids, Some(CredentialScheme::ApiKey));
    let current = meta.client.credential.map(|c| c.hash).expect("credential");
    let previous = ids.credential();
    meta.client.previous_digests = Some(PreviousDigests {
        credential: Some(previous),
        account: None,
    });
    let derived = ChainEvidence::default().derive(&meta, &[]);
    assert!(derived.contains(&IdentityEvidence::StableCredential(current)));
    assert!(derived.contains(&IdentityEvidence::StableCredential(previous)));
    assert_eq!(
        scope_of(&meta.client).previous,
        Some(IdentityScope::Credential(previous))
    );
}

/// The fingerprint is the system prompt plus the first user turn: later
/// messages do not change it, another first turn does.
#[test]
fn prompt_fingerprint_reads_system_and_first_user_turn() {
    let s = message(system_text("prompt"));
    let u = message(user_text("first"));
    let later = message(user_text("later"));
    let other = message(user_text("other first"));
    let a = PromptFingerprintEvidence::fingerprint(&[s.clone(), u.clone()]);
    let b = PromptFingerprintEvidence::fingerprint(&[s.clone(), u, later]);
    let c = PromptFingerprintEvidence::fingerprint(&[s.clone(), other]);
    assert!(a.is_some());
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert_eq!(PromptFingerprintEvidence::fingerprint(&[s]), None);
}

/// The chain orders evidence most specific first.
#[test]
fn chain_orders_most_specific_first() {
    let mut ids = Ids::new();
    let mut meta = meta(&mut ids, Some(CredentialScheme::ApiKey));
    meta.client.ids.agent = Some("sub".to_owned());
    let request = [message(system_text("p")), message(user_text("u"))];
    let derived = ChainEvidence::default().derive(&meta, &request);
    let specificity: Vec<u8> = derived.iter().map(IdentityEvidence::specificity).collect();
    assert_eq!(specificity, vec![5, 4, 2, 1]);
}
