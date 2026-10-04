//! Routing: prefixes and base URLs are checked, the longest prefix wins on
//! whole segments, and an unrouted request is answered 421 locally.

use crosstalk_spec::interfaces::l0_ingress::UpstreamRouter;
use crosstalk_spec::observed::client::IngressMode;
use crosstalk_testkit::client::HarnessClient;
use crosstalk_testkit::upstream::{FakeUpstream, Script};
use hyper::StatusCode;

use super::support::{Options, anthropic_api, case, route, start};
use crate::routing::{ConfigError, Routes};

/// A request outside every route gets a 421 from the proxy and never
/// reaches an upstream; one inside the route does.
#[tokio::test]
async fn unrouted_request_answered_421() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let proxy = start(&upstream.base_url(), Options::default()).await;
    let request = case("text_turn").request;
    for prefix in ["", "/anthropics", "/other"] {
        let response = HarnessClient::new(proxy.addr)
            .prefix(prefix)
            .send(&request)
            .await
            .expect("answered");
        assert_eq!(
            response.status,
            StatusCode::MISDIRECTED_REQUEST,
            "prefix {prefix:?}"
        );
    }
    assert!(upstream.received().await.expect("log").is_empty());
    let _ = proxy.client().send(&request).await.expect("answered");
    assert_eq!(upstream.received().await.expect("log").len(), 1);
}

#[test]
fn routes_match_longest_prefix_on_segments() {
    let mut nested = route("https://example.com/base/", anthropic_api());
    nested.name.0 = "nested".to_owned();
    nested.prefix = "/anthropic/team".to_owned();
    let mut root = route("http://localhost:8000", anthropic_api());
    root.name.0 = "root".to_owned();
    root.prefix = "/".to_owned();
    let routes = Routes::new(&[
        route("https://api.anthropic.com", anthropic_api()),
        nested,
        root,
    ])
    .expect("routes");
    let resolved = routes
        .resolve("/anthropic/team/v1/messages", Some("beta=true"))
        .expect("routed");
    assert_eq!(
        resolved.uri.to_string(),
        "https://example.com/base/v1/messages?beta=true"
    );
    assert_eq!(resolved.upstream_path, "/base/v1/messages");
    let resolved = routes
        .resolve("/anthropic/v1/messages", None)
        .expect("routed");
    assert_eq!(
        resolved.uri.to_string(),
        "https://api.anthropic.com/v1/messages"
    );
    assert!(matches!(resolved.mode, IngressMode::ReverseProxy { route } if route.0 == "anthropic"));
    let resolved = routes
        .resolve("/anthropics/v1", None)
        .expect("the root route");
    assert_eq!(
        resolved.uri.to_string(),
        "http://localhost:8000/anthropics/v1"
    );
    let resolved = routes.resolve("/anthropic", None).expect("the bare prefix");
    assert_eq!(resolved.uri.to_string(), "https://api.anthropic.com/");
    assert_eq!(
        routes.intercept(&crosstalk_spec::derived::flow::resource::Host(
            "api.anthropic.com".to_owned()
        )),
        None
    );
}

#[test]
fn route_configs_are_checked() {
    let with = |prefix: &str, base: &str| {
        let mut config = route(base, anthropic_api());
        config.prefix = prefix.to_owned();
        Routes::new(&[config])
    };
    for prefix in ["anthropic", "/anthropic/", "/a//b", "/a?b"] {
        assert!(
            matches!(
                with(prefix, "https://api.anthropic.com"),
                Err(ConfigError::InvalidPrefix { .. })
            ),
            "{prefix}"
        );
    }
    for base in [
        "api.anthropic.com",
        "ftp://api.anthropic.com",
        "https://user@api.anthropic.com",
        "https://api.anthropic.com/?x=1",
        "https://",
    ] {
        assert!(
            matches!(with("/a", base), Err(ConfigError::InvalidBaseUrl { .. })),
            "{base}"
        );
    }
    let one = route("https://api.anthropic.com", anthropic_api());
    let mut same_name = one.clone();
    same_name.prefix = "/other".to_owned();
    assert_eq!(
        Routes::new(&[one.clone(), same_name]),
        Err(ConfigError::DuplicateName("anthropic".to_owned()))
    );
    let mut same_prefix = one.clone();
    same_prefix.name.0 = "other".to_owned();
    assert_eq!(
        Routes::new(&[one, same_prefix]),
        Err(ConfigError::DuplicatePrefix("/anthropic".to_owned()))
    );
}

#[test]
fn config_parses_and_refuses_unknown_fields() {
    let text = r#"{
        "listen": "127.0.0.1:8080",
        "routes": [{
            "name": "anthropic",
            "prefix": "/anthropic",
            "upstream": {"id": "anthropic", "kind": {"type": "vendor_api", "data": {"type": "anthropic"}}, "base_url": "https://api.anthropic.com"}
        }],
        "secrets": {"current": {"version": 1, "env": "CROSSTALK_SECRET"}},
        "limits": {"upstream_idle_timeout_ms": 600000},
        "capture": {"channel_capacity": 256}
    }"#;
    let config = crate::config::IngressConfig::from_json(text).expect("parses");
    assert_eq!(config.capture.channel_capacity.get(), 256);
    assert_eq!(
        config
            .limits
            .upstream_idle_timeout()
            .map(|idle| idle.as_secs()),
        Some(600)
    );
    assert_eq!(
        config.limits.request_tee_bytes,
        crate::config::LimitsConfig::default().request_tee_bytes
    );
    let lookup =
        |name: &str| (name == "CROSSTALK_SECRET").then(|| super::support::SECRET_HEX.to_owned());
    let (sender, _receiver) = tokio::sync::mpsc::channel(1);
    let built = crate::anthropic_proxy(
        &config,
        lookup,
        crate::capture::CaptureSender::new(sender),
        std::sync::Arc::new(crosstalk_spec::support::SystemClock),
    );
    assert!(built.is_ok());
    let unknown = text.replace("\"listen\"", "\"listne\": 1, \"listen\"");
    assert!(crate::config::IngressConfig::from_json(&unknown).is_err());
    let missing = crate::anthropic_proxy(
        &config,
        |_| None,
        crate::capture::CaptureSender::new(tokio::sync::mpsc::channel(1).0),
        std::sync::Arc::new(crosstalk_spec::support::SystemClock),
    );
    assert!(matches!(missing, Err(crate::BuildError::Secrets(_))));
}
