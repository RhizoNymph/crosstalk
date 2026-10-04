//! The Anthropic adapter: classification, decoding and the dialect.

use std::num::{NonZeroU64, NonZeroUsize};

use crosstalk_spec::interfaces::l0_ingress::{BodyDecodeError, ProviderAdapter, RequestHead};
use crosstalk_spec::observed::client::{
    ClientContext, Dialect, EndpointKind, HarnessIds, InferenceServer, IngressMode, RequestClass,
    RouteName, Upstream, UpstreamId, UpstreamKind, Vendor,
};
use crosstalk_spec::observed::exchange::{Continuation, WireProtocol};
use crosstalk_testkit::upstream::{FakeUpstream, Script};

use super::support::{Options, anthropic_api, case, cases, start};
use crate::adapter::AnthropicAdapter;

fn adapter() -> AnthropicAdapter {
    AnthropicAdapter::new(
        NonZeroU64::MIN.saturating_add(1 << 20),
        NonZeroUsize::MIN.saturating_add(1 << 20),
    )
}

fn client(kind: UpstreamKind) -> ClientContext {
    ClientContext {
        ingress: IngressMode::ReverseProxy {
            route: RouteName("r".to_owned()),
        },
        upstream: Upstream {
            id: UpstreamId("u".to_owned()),
            kind,
        },
        credential: None,
        account: None,
        previous_digests: None,
        harness: None,
        ids: HarnessIds {
            session: None,
            agent: None,
            parent_agent: None,
        },
        class: RequestClass::Unknown,
    }
}

fn head(method: &str, path: &str, headers: &[(&str, &str)]) -> RequestHead {
    RequestHead {
        method: method.to_owned(),
        path: path.to_owned(),
        query: None,
        headers: headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
    }
}

/// The dialect in every captured request is the configured upstream's,
/// whatever the body looks like: through the proxy, per upstream kind.
#[tokio::test]
async fn dialect_follows_configured_upstream() {
    let text = case("text_turn");
    let upstream = FakeUpstream::start(Script::new().case(&text))
        .await
        .expect("upstream");
    let kinds = [
        (anthropic_api(), Dialect::Reference),
        (
            UpstreamKind::Subscription(Vendor::Anthropic),
            Dialect::Reference,
        ),
        (
            UpstreamKind::InferenceServer(InferenceServer::Vllm),
            Dialect::Vllm,
        ),
        (
            UpstreamKind::InferenceServer(InferenceServer::Sglang),
            Dialect::Sglang,
        ),
        (
            UpstreamKind::Subscription(Vendor::GithubCopilot),
            Dialect::Copilot,
        ),
    ];
    for (kind, dialect) in kinds {
        let mut proxy = start(
            &upstream.base_url(),
            Options {
                kind: kind.clone(),
                ..Options::default()
            },
        )
        .await;
        let _ = proxy.client().send(&text.request).await.expect("answered");
        let raw = proxy.next_capture().await.expect("captured");
        assert_eq!(raw.request.harness.dialect, dialect, "{kind:?}");
        assert_eq!(raw.request.harness.dialect, kind.dialect());
        assert_eq!(raw.meta.client.upstream.kind, kind);
    }
}

/// The Anthropic rows of the endpoint table, and the corpus's own
/// endpoint kinds.
#[test]
fn endpoint_table_classification() {
    let adapter = adapter();
    let versioned = [("anthropic-version", "2023-06-01")];
    let table = [
        (
            head("POST", "/v1/messages", &versioned),
            Some(EndpointKind::Generation),
        ),
        (
            head("POST", "/v1/messages", &[]),
            Some(EndpointKind::Generation),
        ),
        (
            head("POST", "/v1/messages/count_tokens", &versioned),
            Some(EndpointKind::TokenCount),
        ),
        (
            head("POST", "/v1/messages/batches", &versioned),
            Some(EndpointKind::Other),
        ),
        (
            head("GET", "/v1/messages", &versioned),
            Some(EndpointKind::Other),
        ),
        (
            head("GET", "/v1/models", &versioned),
            Some(EndpointKind::ModelList),
        ),
        (
            head("GET", "/v1/models/claude-opus-5-5", &versioned),
            Some(EndpointKind::ModelList),
        ),
        (head("GET", "/v1/models", &[]), None),
        (head("HEAD", "/api/hello", &[]), Some(EndpointKind::Probe)),
        (head("GET", "/api/hello", &[]), Some(EndpointKind::Probe)),
        (
            head("POST", "/v1/files", &versioned),
            Some(EndpointKind::Other),
        ),
        (head("POST", "/v1/chat/completions", &[]), None),
        (head("POST", "/v1/responses", &[]), None),
    ];
    for (head, kind) in table {
        assert_eq!(
            adapter.classify(&head),
            kind,
            "{} {}",
            head.method,
            head.path
        );
    }
    for case in cases() {
        assert_eq!(
            adapter.classify(&case.request.head()),
            Some(case.meta.endpoint.kind()),
            "case {}",
            case.name
        );
    }
    assert_eq!(adapter.protocol(), WireProtocol::AnthropicMessages);
}

/// Decoding reads model, stream and the continuation, and refuses what is
/// not a Messages body, naming why.
#[test]
fn decode_reads_fields_and_refuses_bad_bodies() {
    let adapter = adapter();
    let ok = head(
        "POST",
        "/v1/messages",
        &[("anthropic-version", "2023-06-01")],
    );
    let context = client(anthropic_api());
    for case in cases()
        .into_iter()
        .filter(|case| case.meta.endpoint.kind() == EndpointKind::Generation)
    {
        let decoded = adapter
            .decode_request(&case.request.head(), &case.request.body, &context)
            .expect("decodes");
        let json = case.request.json().expect("json");
        assert_eq!(decoded.model.0, json["model"].as_str().unwrap_or_default());
        assert_eq!(decoded.stream, json["stream"].as_bool().unwrap_or(false));
        assert_eq!(decoded.continuation, Continuation::FullHistory);
        assert_eq!(decoded.protocol, WireProtocol::AnthropicMessages);
    }
    let body = b"{\"model\": \"m\",\n \"messages\": [}";
    assert_eq!(
        adapter.decode_request(&ok, body, &context),
        Err(BodyDecodeError::NotJson { offset: 29 })
    );
    assert_eq!(
        adapter.decode_request(&ok, b"{\"messages\": []}", &context),
        Err(BodyDecodeError::MissingField("model"))
    );
    assert_eq!(
        adapter.decode_request(&ok, b"{\"model\": \"m\"}", &context),
        Err(BodyDecodeError::MissingField("messages"))
    );
    let future = head(
        "POST",
        "/v1/messages",
        &[("anthropic-version", "2031-01-01")],
    );
    assert_eq!(
        adapter.decode_request(&future, b"{\"model\": \"m\", \"messages\": []}", &context),
        Err(BodyDecodeError::UnsupportedVersion("2031-01-01".to_owned()))
    );
    let brotli = head("POST", "/v1/messages", &[("content-encoding", "br")]);
    assert!(matches!(
        adapter.decode_request(&brotli, b"\x0b\x02\x80", &context),
        Err(BodyDecodeError::UnsupportedEncoding(_))
    ));
    let gzip = head("POST", "/v1/messages", &[("content-encoding", "gzip")]);
    assert!(matches!(
        adapter.decode_request(&gzip, b"not gzip", &context),
        Err(BodyDecodeError::UnsupportedEncoding(_))
    ));
}
