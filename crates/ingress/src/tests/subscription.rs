//! Claude Code on a Claude Pro/Max subscription: a Bearer OAuth access
//! token sent with the OAuth capability in `anthropic-beta`, through the
//! proxy unchanged, classified `OauthAccessToken` and kept only as a keyed
//! digest.
//!
//! Every token here is fake and built in code (`sk-ant-oat01-TEST…`, or an
//! opaque `TEST…` text standing in for a future token format).

use crosstalk_spec::interfaces::l0_ingress::{ClientIdentifier, RequestHead};
use crosstalk_spec::observed::client::{
    CredentialScheme, InferenceServer, Upstream, UpstreamId, UpstreamKind, Vendor,
};
use crosstalk_spec::support::Timestamp;
use crosstalk_testkit::upstream::{FakeUpstream, Script};
use proptest::prelude::*;

use super::support::{Options, anthropic_api, case, identifier, start};
use crate::identify::OauthCapability;

const AT: Timestamp = Timestamp::from_micros(1_790_000_000_000_000);

/// A fake subscription access token, shaped like the real ones.
const FAKE_TOKEN: &str = "sk-ant-oat01-TEST-fake-access-token-0123456789abcdef";
/// The same session's token after a (simulated) refresh.
const FAKE_REFRESHED: &str = "sk-ant-oat01-TEST-fake-refreshed-token-fedcba9876543210";
/// A fake token in a shape the shape rule does not know.
const FAKE_OPAQUE: &str = "TEST-opaque-subscription-token-0123456789";

const OAUTH_BETAS: &str = "claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14";

fn upstream(kind: UpstreamKind) -> Upstream {
    Upstream {
        id: UpstreamId("anthropic".to_owned()),
        kind,
    }
}

fn head(headers: &[(&str, &str)]) -> RequestHead {
    RequestHead {
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: Some("beta=true".to_owned()),
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
    }
}

fn scheme_of(headers: &[(&str, &str)], kind: UpstreamKind) -> Option<CredentialScheme> {
    identifier()
        .credential(&head(headers), &upstream(kind), AT)
        .map(|credential| credential.scheme)
}

/// `ingress.credential.oauth-capability-marks-oauth`: on an Anthropic
/// upstream a Bearer credential sent with an `anthropic-beta` value that
/// starts `oauth-` is `OauthAccessToken` whatever its shape; a key header
/// never is; other betas, and other vendors, leave the shape rule alone.
#[test]
fn oauth_capability_marks_oauth() {
    let bearer = format!("Bearer {FAKE_OPAQUE}");
    let anthropic = [
        anthropic_api(),
        UpstreamKind::Subscription(Vendor::Anthropic),
    ];
    for kind in &anthropic {
        // The documented signal alone, on a token of unknown shape.
        assert_eq!(
            scheme_of(
                &[("authorization", &bearer), ("anthropic-beta", OAUTH_BETAS)],
                kind.clone()
            ),
            Some(CredentialScheme::OauthAccessToken),
            "{kind:?}"
        );
        // A key header with the OAuth beta is still a key.
        assert_eq!(
            scheme_of(
                &[("x-api-key", FAKE_OPAQUE), ("anthropic-beta", OAUTH_BETAS)],
                kind.clone()
            ),
            Some(CredentialScheme::ApiKey),
            "{kind:?}"
        );
    }
    // Without the capability, an opaque Bearer on the vendor API is a key
    // (INV-17), and an `sk-ant-oat` token is OAuth by its shape.
    for betas in [
        None,
        Some("claude-code-20250219"),
        Some("xoauth-1,oauth"),
        Some(""),
    ] {
        let mut headers = vec![("authorization", bearer.as_str())];
        if let Some(betas) = betas {
            headers.push(("anthropic-beta", betas));
        }
        assert_eq!(
            scheme_of(&headers, anthropic_api()),
            Some(CredentialScheme::ApiKey),
            "{betas:?}"
        );
    }
    let shaped = format!("Bearer {FAKE_TOKEN}");
    assert_eq!(
        scheme_of(&[("authorization", &shaped)], anthropic_api()),
        Some(CredentialScheme::OauthAccessToken)
    );
    // Case, whitespace and a second header line are all read.
    for headers in [
        vec![
            ("authorization", bearer.as_str()),
            ("Anthropic-Beta", " OAuth-2025-04-20 "),
        ],
        vec![
            ("authorization", bearer.as_str()),
            ("anthropic-beta", "claude-code-20250219"),
            ("anthropic-beta", "a-1 , oauth-2099-01-01"),
        ],
    ] {
        assert_eq!(
            scheme_of(&headers, anthropic_api()),
            Some(CredentialScheme::OauthAccessToken),
            "{headers:?}"
        );
    }
    // The capability is Anthropic's: other upstreams keep their rule.
    for (kind, expected) in [
        (
            UpstreamKind::VendorApi(Vendor::OpenAi),
            CredentialScheme::ApiKey,
        ),
        (
            UpstreamKind::InferenceServer(InferenceServer::Vllm),
            CredentialScheme::ServerKey,
        ),
        (
            UpstreamKind::VendorApi(Vendor::GithubCopilot),
            CredentialScheme::ExchangedToken,
        ),
    ] {
        assert_eq!(
            scheme_of(
                &[("authorization", &bearer), ("anthropic-beta", OAUTH_BETAS)],
                kind.clone()
            ),
            Some(expected),
            "{kind:?}"
        );
    }
}

#[test]
fn oauth_capability_reads_the_header_only() {
    assert_eq!(
        OauthCapability::of(&head(&[("anthropic-beta", OAUTH_BETAS)])),
        OauthCapability::Sent
    );
    assert_eq!(OauthCapability::of(&head(&[])), OauthCapability::NotSent);
    assert_eq!(
        OauthCapability::of(&head(&[("x-anthropic-beta", "oauth-2025-04-20")])),
        OauthCapability::NotSent
    );
}

fn beta_value() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9-]{0,20}".prop_filter("not an oauth value", |value| !value.starts_with("oauth-"))
}

proptest! {
    /// Wherever the OAuth value sits among other betas, in any case and
    /// with any padding, a Bearer of any shape on the Anthropic API is
    /// `OauthAccessToken`; without it, an opaque Bearer is `ApiKey`.
    #[test]
    fn oauth_capability_marks_oauth_anywhere(
        others in proptest::collection::vec(beta_value(), 0..6),
        position in any::<prop::sample::Index>(),
        suffix in "[0-9a-z-]{0,12}",
        upper in any::<bool>(),
        pad in " {0,2}",
        token in "TEST[A-Za-z0-9_-]{4,40}",
    ) {
        let oauth = format!("{pad}{}{suffix}{pad}", if upper { "OAUTH-" } else { "oauth-" });
        let mut values = others.clone();
        values.insert(position.index(values.len() + 1), oauth);
        let with = values.join(",");
        let without = others.join(",");
        let bearer = format!("Bearer {token}");
        prop_assert_eq!(
            scheme_of(&[("authorization", &bearer), ("anthropic-beta", &with)], anthropic_api()),
            Some(CredentialScheme::OauthAccessToken)
        );
        prop_assert_eq!(
            scheme_of(&[("authorization", &bearer), ("anthropic-beta", &without)], anthropic_api()),
            Some(CredentialScheme::ApiKey)
        );
        prop_assert_eq!(
            scheme_of(&[("x-api-key", &token), ("anthropic-beta", &with)], anthropic_api()),
            Some(CredentialScheme::ApiKey)
        );
    }
}

/// A Claude Code subscription exchange through the proxy, before and after
/// a token refresh: the upstream receives each request unchanged (the
/// Bearer token and the OAuth beta included), the client receives the
/// stream and the unified rate-limit headers unchanged, and each
/// RawExchange holds the token only as an `OauthAccessToken` digest, a
/// different one after the refresh, with the same harness session.
#[tokio::test]
async fn subscription_session_passes_through_and_is_captured() {
    let oauth = case("subagent_oauth_streaming");
    assert!(
        oauth
            .request
            .headers
            .get_str("anthropic-beta")
            .is_some_and(|betas| betas.contains("oauth-")),
        "the corpus case carries the OAuth capability"
    );
    let upstream = FakeUpstream::start(Script::new().case(&oauth))
        .await
        .expect("upstream");
    let mut proxy = start(&upstream.base_url(), Options::default()).await;
    let mut hashes = Vec::new();
    let mut sessions = Vec::new();
    for token in [FAKE_TOKEN, FAKE_REFRESHED] {
        let request = oauth
            .request_with_credential(token)
            .expect("a header value");
        let response = proxy.client().send(&request).await.expect("answered");
        assert_eq!(response.differences_from(&oauth.response), Vec::new());
        assert_eq!(
            response
                .headers
                .get_str("anthropic-ratelimit-unified-status"),
            Some("allowed")
        );
        let received = upstream.received().await.expect("log");
        let last = received.last().expect("the upstream got the request");
        assert_eq!(last.differences_from(&request), Vec::new());

        let raw = proxy.next_capture().await.expect("captured");
        let credential = raw.meta.client.credential.expect("a credential ref");
        assert_eq!(credential.scheme, CredentialScheme::OauthAccessToken);
        hashes.push(credential.hash);
        sessions.push(raw.meta.client.ids.session.clone());
        let debug = format!("{raw:?}");
        let json = serde_json::to_string(&raw.meta).expect("meta serializes");
        for text in [FAKE_TOKEN, FAKE_REFRESHED, "TEST-fake"] {
            assert!(!debug.contains(text), "{text} is in the RawExchange");
            assert!(!json.contains(text), "{text} is in the exchange meta");
        }
    }
    assert_ne!(hashes[0], hashes[1], "a refreshed token has its own digest");
    assert!(sessions[0].is_some());
    assert_eq!(
        sessions[0], sessions[1],
        "one harness session across the refresh"
    );
}
