//! A proxy on a real loopback socket, in front of testkit's `FakeUpstream`,
//! driven by testkit's `HarnessClient`.

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_spec::ids::{
    DeploymentSecret, KeyedHasher, SecretVersion, SeededRandom, UlidGenerator,
};
use crosstalk_spec::interfaces::l0_ingress::RawExchange;
use crosstalk_spec::observed::client::{RouteName, UpstreamId, UpstreamKind, Vendor};
use crosstalk_spec::support::SystemClock;
use crosstalk_testkit::client::HarnessClient;
use crosstalk_testkit::corpus::{Case, anthropic};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::adapter::AnthropicAdapter;
use crate::capture::{CaptureSender, CaptureStats};
use crate::config::{LimitsConfig, RouteConfig, UpstreamConfig};
use crate::decode::AdapterDecoder;
use crate::exchange::StageEvent;
use crate::identify::HeaderIdentifier;
use crate::proxy::{Proxy, ProxyParts, connector};
use crate::routing::Routes;

/// The route prefix every test harness uses as its base URL path.
pub const PREFIX: &str = "/anthropic";

/// The test deployment secret, version 7.
pub const SECRET_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
pub const SECRET_VERSION: SecretVersion = SecretVersion(7);

pub fn keys() -> KeyedHasher {
    KeyedHasher::new(
        DeploymentSecret::from_hex(SECRET_VERSION, SECRET_HEX).expect("the test secret is hex"),
    )
}

pub fn identifier() -> HeaderIdentifier {
    HeaderIdentifier::new(keys())
}

pub fn anthropic_api() -> UpstreamKind {
    UpstreamKind::VendorApi(Vendor::Anthropic)
}

pub fn route(base_url: &str, kind: UpstreamKind) -> RouteConfig {
    RouteConfig {
        name: RouteName("anthropic".to_owned()),
        prefix: PREFIX.to_owned(),
        upstream: UpstreamConfig {
            id: UpstreamId("anthropic".to_owned()),
            kind,
            base_url: base_url.to_owned(),
        },
    }
}

pub fn adapter(limits: &LimitsConfig) -> Arc<AnthropicAdapter> {
    Arc::new(AnthropicAdapter::new(
        limits.decoded_bytes,
        limits.sse_event_bytes,
    ))
}

pub fn cases() -> Vec<Case> {
    anthropic::cases().expect("the corpus loads")
}

pub fn case(name: &str) -> Case {
    anthropic::case(name).expect("the case loads")
}

pub fn non_zero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero")
}

/// How a test proxy is set up.
#[derive(Debug, Clone)]
pub struct Options {
    pub kind: UpstreamKind,
    pub capacity: usize,
    pub limits: LimitsConfig,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            kind: anthropic_api(),
            capacity: 64,
            limits: LimitsConfig::default(),
        }
    }
}

/// A running proxy. Dropping it stops the accept loop.
pub struct TestProxy {
    pub addr: SocketAddr,
    pub captured: mpsc::Receiver<RawExchange>,
    pub stages: mpsc::Receiver<StageEvent>,
    pub stats: Arc<CaptureStats>,
    _shutdown: oneshot::Sender<()>,
    _server: JoinHandle<()>,
}

impl TestProxy {
    /// A harness whose base URL is this proxy's route.
    pub fn client(&self) -> HarnessClient {
        HarnessClient::new(self.addr).prefix(PREFIX)
    }

    /// The next captured exchange, waiting at most five seconds.
    pub async fn next_capture(&mut self) -> Option<RawExchange> {
        tokio::time::timeout(Duration::from_secs(5), self.captured.recv())
            .await
            .ok()
            .flatten()
    }

    /// Wait until `stats` shows `done` exchanges accounted for (captured or
    /// counted uncaptured).
    pub async fn settle(&self, done: u64) {
        for _ in 0..500 {
            let counts = self.stats.snapshot();
            let total = counts.captured
                + counts.decode_error
                + counts.channel_full
                + counts.channel_closed
                + counts.response_too_large
                + counts.ids_exhausted;
            if total >= done {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Start a proxy routing [`PREFIX`] to `upstream_base`.
pub async fn start(upstream_base: &str, options: Options) -> TestProxy {
    let routes = Routes::new(&[route(upstream_base, options.kind.clone())]).expect("valid route");
    let (sender, captured) = mpsc::channel(options.capacity);
    let (observer, stages) = mpsc::channel(4096);
    let adapter = adapter(&options.limits);
    let proxy = Proxy::new(ProxyParts {
        routes,
        identifier: identifier(),
        decoder: AdapterDecoder::new(Arc::clone(&adapter), options.limits.decoded_bytes),
        adapter,
        connector: connector::https(),
        capture: CaptureSender::new(sender),
        clock: Arc::new(SystemClock),
        ids: UlidGenerator::new(Arc::new(SystemClock), SeededRandom::new(1)),
        limits: options.limits,
        observer: Some(observer),
    });
    let stats = proxy.stats();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("address");
    let (shutdown, stop) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let _ = proxy
            .serve(listener, async move {
                let _ = stop.await;
            })
            .await;
    });
    TestProxy {
        addr,
        captured,
        stages,
        stats,
        _shutdown: shutdown,
        _server: server,
    }
}

pub fn limits_with(update: impl FnOnce(&mut LimitsConfig)) -> LimitsConfig {
    let mut limits = LimitsConfig::default();
    update(&mut limits);
    limits
}
