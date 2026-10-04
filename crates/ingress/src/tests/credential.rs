//! Credentials and accounts: hashed at once, keyed with the current secret,
//! schemes by the documented rule, and never in a RawExchange, a log line or
//! an error.

use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use crosstalk_spec::ids::SecretVersion;
use crosstalk_spec::interfaces::l0_ingress::{ClientIdentifier, RequestHead};
use crosstalk_spec::observed::client::{
    CredentialScheme, InferenceServer, IngressMode, RouteName, Upstream, UpstreamId, UpstreamKind,
    Vendor,
};
use crosstalk_testkit::upstream::{FakeUpstream, Script};
use hyper::header::{HeaderName, HeaderValue};
use proptest::prelude::*;

use super::support::{
    Options, SECRET_HEX, SECRET_VERSION, anthropic_api, case, cases, identifier, keys, start,
};
use crate::config::{SecretRef, SecretsConfig};
use crate::credential::{
    DeploymentSecret, KeyedHasher, RawCredential, SameVersion, SecretError, load_secrets,
};
use crate::identify::HeaderIdentifier;

fn upstream(kind: UpstreamKind) -> Upstream {
    Upstream {
        id: UpstreamId("test".to_owned()),
        kind,
    }
}

fn head(headers: &[(&str, &str)], query: Option<&str>) -> RequestHead {
    RequestHead {
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query: query.map(str::to_owned),
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
    }
}

/// One scheme's fixture: the upstream kind, the header carrying the
/// credential and its value, and the secret text inside it.
struct Fixture {
    kind: UpstreamKind,
    header: &'static str,
    value: &'static str,
    secret: &'static str,
    scheme: CredentialScheme,
}

fn fixtures() -> Vec<Fixture> {
    vec![
        Fixture {
            kind: anthropic_api(),
            header: "x-api-key",
            value: "sk-ant-api03-SECRETAPIKEY0123456789",
            secret: "SECRETAPIKEY0123456789",
            scheme: CredentialScheme::ApiKey,
        },
        Fixture {
            kind: anthropic_api(),
            header: "authorization",
            value: "Bearer sk-ant-oat01-SECRETOAUTH0123456789",
            secret: "SECRETOAUTH0123456789",
            scheme: CredentialScheme::OauthAccessToken,
        },
        Fixture {
            kind: UpstreamKind::Subscription(Vendor::GithubCopilot),
            header: "authorization",
            value: "Bearer tid=SECRETCOPILOT0123456789;exp=1790830800",
            secret: "SECRETCOPILOT0123456789",
            scheme: CredentialScheme::ExchangedToken,
        },
        Fixture {
            kind: UpstreamKind::InferenceServer(InferenceServer::Vllm),
            header: "authorization",
            value: "Bearer SECRETSERVERKEY0123456789",
            secret: "SECRETSERVERKEY0123456789",
            scheme: CredentialScheme::ServerKey,
        },
    ]
}

const ACCOUNT: &str = "SECRETACCOUNT-0f9e8d7c";

/// The text case's request with `fixture`'s credential and an account id.
fn request_for(fixture: &Fixture) -> crosstalk_testkit::corpus::CorpusRequest {
    let case = case("text_turn");
    let mut request = case.request.clone();
    let mut headers = crosstalk_testkit::corpus::http::Headers::new();
    for (name, value) in case.request.headers.iter() {
        if name.as_str() != "x-api-key" && name.as_str() != "authorization" {
            headers.push(name.clone(), value.clone());
        }
    }
    headers.push(
        HeaderName::from_static(fixture.header),
        HeaderValue::from_static(fixture.value),
    );
    headers.push(
        HeaderName::from_static("chatgpt-account-id"),
        HeaderValue::from_static(ACCOUNT),
    );
    request.headers = headers;
    request
}

/// Through the proxy, for every scheme: the RawExchange carries the
/// credential and account only as keyed digests; their raw text appears
/// nowhere in it.
#[tokio::test]
async fn raw_credential_absent_from_raw_exchange_per_scheme() {
    let text = case("text_turn");
    for fixture in fixtures() {
        let upstream = FakeUpstream::start(Script::new().case(&text))
            .await
            .expect("upstream");
        let mut proxy = start(
            &upstream.base_url(),
            Options {
                kind: fixture.kind.clone(),
                ..Options::default()
            },
        )
        .await;
        let request = request_for(&fixture);
        let _ = proxy.client().send(&request).await.expect("answered");
        let raw = proxy.next_capture().await.expect("captured");
        let credential = raw.meta.client.credential.expect("a credential ref");
        assert_eq!(credential.scheme, fixture.scheme);
        assert_eq!(credential.hash.key(), SECRET_VERSION);
        assert!(raw.meta.client.account.is_some());
        let debug = format!("{raw:?}");
        let json = serde_json::to_string(&raw.meta).expect("meta serializes");
        for text in [fixture.secret, fixture.value, ACCOUNT] {
            assert!(!debug.contains(text), "{text} is in the RawExchange");
            assert!(!json.contains(text), "{text} is in the exchange meta");
        }
        assert!(!String::from_utf8_lossy(&raw.request.body).contains(fixture.secret));
    }
}

/// A shared log buffer for a capturing subscriber.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(PoisonError::into_inner)).into_owned()
    }
}

/// Every log record the proxy writes at any level, through success,
/// decode failure, an unreachable upstream and an unrouted request, and
/// every error and debug value that could carry a credential or the
/// deployment secret, hold neither.
#[test]
fn raw_credential_and_secret_absent_from_logs_and_errors() {
    // Global, not scoped: a scoped subscriber misses events from callsites
    // whose interest other test threads cached first. Every test's logs
    // land here, which only widens the check.
    static LOGS: OnceLock<Captured> = OnceLock::new();
    let captured = LOGS
        .get_or_init(|| {
            let captured = Captured::default();
            let writer = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_writer(move || writer.clone())
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
            captured
        })
        .clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let text = case("text_turn");
        for fixture in fixtures() {
            let upstream = FakeUpstream::start(Script::new().case(&text))
                .await
                .expect("upstream");
            let mut proxy = start(
                &upstream.base_url(),
                Options {
                    kind: fixture.kind.clone(),
                    ..Options::default()
                },
            )
            .await;
            let mut request = request_for(&fixture);
            let _ = proxy.client().send(&request).await.expect("answered");
            let _ = proxy.next_capture().await;
            request.body = bytes::Bytes::from_static(b"not json");
            let _ = proxy.client().send(&request).await.expect("answered");
            proxy.settle(2).await;
            let _ = crosstalk_testkit::client::HarnessClient::new(proxy.addr)
                .send(&request)
                .await;
            drop(upstream);
            let _ = proxy.client().send(&request).await;
            proxy.settle(3).await;
        }
    });
    let logs = captured.text();
    assert!(
        logs.contains("not captured"),
        "the capture produced no logs: {logs}"
    );
    assert!(logs.contains("upstream unreachable"), "{logs}");
    for fixture in fixtures() {
        assert!(
            !logs.contains(fixture.secret),
            "{} is in the logs",
            fixture.secret
        );
    }
    assert!(!logs.contains(ACCOUNT));
    assert!(!logs.contains(SECRET_HEX));

    let raw = RawCredential::new("sk-ant-api03-SECRETAPIKEY0123456789").expect("non-empty");
    assert!(!format!("{raw:?}").contains("SECRET"));
    let secret = DeploymentSecret::from_hex(SECRET_HEX).expect("hex");
    assert!(!format!("{secret:?}").contains("0001020304"));
    assert!(!format!("{:?}", keys()).contains("0001020304"));
    assert!(!format!("{:?}", identifier()).contains("0001020304"));
    let config = SecretsConfig {
        current: SecretRef {
            version: SecretVersion(1),
            env: "CROSSTALK_SECRET".to_owned(),
        },
        previous: None,
    };
    let malformed = load_secrets(&config, |_| Some("zz-not-a-secret-SECRETVALUE".to_owned()))
        .expect_err("not hex");
    assert!(matches!(malformed, SecretError::Malformed { .. }));
    for shown in [format!("{malformed}"), format!("{malformed:?}")] {
        assert!(!shown.contains("SECRETVALUE"), "{shown}");
    }
}

/// Every credential form a harness uses yields a CredentialRef, on every
/// kind of upstream.
#[test]
fn every_scheme_yields_credential_ref() {
    let identifier = identifier();
    let kinds = [
        anthropic_api(),
        UpstreamKind::VendorApi(Vendor::OpenAi),
        UpstreamKind::Subscription(Vendor::Anthropic),
        UpstreamKind::Subscription(Vendor::OpenAi),
        UpstreamKind::Subscription(Vendor::Google),
        UpstreamKind::Subscription(Vendor::GithubCopilot),
        UpstreamKind::InferenceServer(InferenceServer::Vllm),
        UpstreamKind::InferenceServer(InferenceServer::Sglang),
    ];
    let forms: Vec<RequestHead> = vec![
        head(&[("x-api-key", "sk-ant-api03-abc")], None),
        head(&[("authorization", "Bearer sk-ant-oat01-abc")], None),
        head(
            &[("authorization", "Bearer eyJhbGciOi.eyJzdWIiOi.c2ln")],
            None,
        ),
        head(&[("Authorization", "bearer tid=abc;exp=1")], None),
        head(&[("authorization", "Basic dXNlcjpwYXNz")], None),
        head(&[("x-goog-api-key", "AIzaSyabc")], None),
        head(&[("api-key", "azure-key")], None),
        head(&[], Some("alt=sse&key=AIzaSyabc")),
    ];
    for kind in &kinds {
        for form in &forms {
            let credential = identifier.credential(form, &upstream(kind.clone()));
            assert!(
                credential.is_some(),
                "{form:?} on {kind:?} gave no credential"
            );
        }
    }
    assert_eq!(
        identifier.credential(&head(&[], None), &upstream(anthropic_api())),
        None
    );
}

fn present(form: usize, credential: &str, extra: &[(String, String)]) -> RequestHead {
    let mut headers: Vec<(String, String)> = extra.to_vec();
    let mut query = None;
    match form {
        0 => headers.push(("x-api-key".to_owned(), credential.to_owned())),
        1 => headers.push(("authorization".to_owned(), format!("Bearer {credential}"))),
        2 => headers.push(("x-goog-api-key".to_owned(), credential.to_owned())),
        3 => headers.push(("api-key".to_owned(), credential.to_owned())),
        _ => query = Some(format!("key={credential}")),
    }
    RequestHead {
        method: "POST".to_owned(),
        path: "/v1/messages".to_owned(),
        query,
        headers,
    }
}

proptest! {
    /// For one secret version, the digest depends only on the credential:
    /// not on the header or parameter carrying it, the scheme, the upstream,
    /// the rest of the request or the node computing it.
    #[test]
    fn hash_stable_under_request_changes(
        credential in "[A-Za-z0-9_-]{1,40}",
        form in 0usize..5,
        extra in proptest::collection::vec(("x-[a-z]{1,8}", "[a-z0-9]{0,8}"), 0..5),
        kind in 0usize..4,
    ) {
        let kinds = [
            anthropic_api(),
            UpstreamKind::Subscription(Vendor::OpenAi),
            UpstreamKind::Subscription(Vendor::GithubCopilot),
            UpstreamKind::InferenceServer(InferenceServer::Sglang),
        ];
        let node_a = identifier();
        let node_b = HeaderIdentifier::new(keys());
        let reference = node_a
            .credential(&present(0, &credential, &[]), &upstream(anthropic_api()))
            .map(|credential| credential.hash);
        let changed = node_b
            .credential(&present(form, &credential, &extra), &upstream(kinds[kind].clone()))
            .map(|credential| credential.hash);
        prop_assert!(reference.is_some());
        prop_assert_eq!(reference, changed);
    }
}

/// Every digest in a ClientContext is keyed with the current secret
/// version; during a rotation overlap the previous version's digests are
/// recorded beside them, and differ.
#[test]
fn digests_use_current_secret_version() {
    let current = DeploymentSecret::from_hex(SECRET_HEX).expect("hex");
    let previous = DeploymentSecret::from_bytes([9; 32]);
    let rotating = HeaderIdentifier::new(
        KeyedHasher::new(
            (SecretVersion(8), current),
            Some((SecretVersion(7), previous)),
        )
        .expect("two versions"),
    );
    let request = head(
        &[
            ("authorization", "Bearer sk-ant-oat01-abc"),
            ("chatgpt-account-id", "acct-1"),
        ],
        None,
    );
    let mode = IngressMode::ReverseProxy {
        route: RouteName("r".to_owned()),
    };
    let context = rotating.context(&request, mode.clone(), upstream(anthropic_api()));
    let credential = context.credential.expect("credential");
    let account = context.account.expect("account");
    assert_eq!(credential.hash.key(), SecretVersion(8));
    assert_eq!(account.key(), SecretVersion(8));
    let before = context.previous_digests.expect("an overlap");
    let old_credential = before.credential.expect("previous credential digest");
    let old_account = before.account.expect("previous account digest");
    assert_eq!(old_credential.key(), SecretVersion(7));
    assert_eq!(old_account.key(), SecretVersion(7));
    assert_ne!(old_credential.digest(), credential.hash.digest());
    let single = identifier().context(&request, mode, upstream(anthropic_api()));
    assert_eq!(
        single.credential.map(|c| c.hash.key()),
        Some(SECRET_VERSION)
    );
    assert_eq!(single.previous_digests, None);
    assert_eq!(
        KeyedHasher::new(
            (SecretVersion(1), DeploymentSecret::from_bytes([1; 32])),
            Some((SecretVersion(1), DeploymentSecret::from_bytes([2; 32])))
        ),
        Err(SameVersion(SecretVersion(1)))
    );
    // The digest is the plain keyed BLAKE3 of the raw value: the
    // semantics the spec's keyed hasher has (`canonical.ids.secret-digest-keyed`).
    let raw = b"sk-ant-oat01-abc";
    let expected = blake3::keyed_hash(
        &[
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ],
        raw,
    );
    assert_eq!(
        keys().credential(raw).digest().as_bytes(),
        expected.as_bytes()
    );
    assert_ne!(
        keys().credential(raw).digest().as_bytes(),
        blake3::hash(raw).as_bytes()
    );
}

/// The scheme of every corpus case is the one its metadata records, and
/// the other harnesses' credentials follow the documented rule.
#[test]
fn scheme_rule_per_harness_fixture() {
    let identifier = identifier();
    for case in cases() {
        let expected = case
            .meta
            .credential
            .as_ref()
            .map(|credential| credential.scheme);
        let got = identifier
            .credential(&case.request.head(), &upstream(anthropic_api()))
            .map(|credential| credential.scheme);
        assert_eq!(got, expected, "case {}", case.name);
    }
    let table = [
        // Codex on a ChatGPT login: a JWT bearer on the subscription backend.
        (
            UpstreamKind::Subscription(Vendor::OpenAi),
            "authorization",
            "Bearer eyJhbGciOi.eyJzdWIiOi.c2ln",
            CredentialScheme::OauthAccessToken,
        ),
        // Codex with an API key.
        (
            UpstreamKind::VendorApi(Vendor::OpenAi),
            "authorization",
            "Bearer sk-proj-abc",
            CredentialScheme::ApiKey,
        ),
        // pi on Copilot: a minted token.
        (
            UpstreamKind::Subscription(Vendor::GithubCopilot),
            "authorization",
            "Bearer tid=abc;exp=1",
            CredentialScheme::ExchangedToken,
        ),
        // oh-my-pi on Gemini CLI: a Google OAuth bearer.
        (
            UpstreamKind::Subscription(Vendor::Google),
            "authorization",
            "Bearer ya29.a0abc",
            CredentialScheme::OauthAccessToken,
        ),
        // pi and oh-my-pi on Claude OAuth, routed as the API host.
        (
            anthropic_api(),
            "authorization",
            "Bearer sk-ant-oat01-abc",
            CredentialScheme::OauthAccessToken,
        ),
        // Claude Code with ANTHROPIC_AUTH_TOKEN.
        (
            anthropic_api(),
            "authorization",
            "Bearer sk-ant-api03-abc",
            CredentialScheme::ApiKey,
        ),
        // A self-hosted server's shared key.
        (
            UpstreamKind::InferenceServer(InferenceServer::Vllm),
            "authorization",
            "Bearer local-key",
            CredentialScheme::ServerKey,
        ),
        (
            UpstreamKind::InferenceServer(InferenceServer::Sglang),
            "x-api-key",
            "local-key",
            CredentialScheme::ServerKey,
        ),
        // A key header on a subscription upstream is still a key.
        (
            UpstreamKind::Subscription(Vendor::Anthropic),
            "x-api-key",
            "sk-ant-api03-abc",
            CredentialScheme::ApiKey,
        ),
    ];
    for (kind, header, value, scheme) in table {
        let got = identifier
            .credential(&head(&[(header, value)], None), &upstream(kind.clone()))
            .map(|credential| credential.scheme);
        assert_eq!(got, Some(scheme), "{header}: {value} on {kind:?}");
    }
}
