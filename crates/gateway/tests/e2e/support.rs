//! A gateway on loopback sockets in front of testkit's `FakeUpstream`,
//! driven by testkit's `HarnessClient`, with a test consumer on the bus.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_gateway::config::{GatewayConfig, exchange_log_path};
use crosstalk_gateway::gateway::{self, Running};
use crosstalk_gateway::role::Role;
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::MessageHash;
use crosstalk_spec::interfaces::l2_transport::{ConsumerGroup, EventBus, Subscription};
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_spec::support::SystemClock;
use crosstalk_testkit::client::{CollectedResponse, HarnessClient};
use crosstalk_testkit::corpus::{Case, CorpusRequest, Endpoint, Headers, anthropic};
use crosstalk_transport::{BusConfig, MpscSubscription};
use hyper::Method;
use hyper::http::uri::PathAndQuery;
use serde_json::Value;
use tempfile::TempDir;

/// The route prefix: `ANTHROPIC_BASE_URL=http://<proxy>/anthropic`.
pub const PREFIX: &str = "/anthropic";

/// The variable the test config names for the deployment secret.
pub const SECRET_ENV: &str = "CROSSTALK_TEST_SECRET_V1";

/// The test deployment secret. Distinctive, so a log test can look for it.
pub const SECRET_HEX: &str = "5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2";

/// How a test gateway is set up.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub drain_ms: u64,
    pub flush_ms: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            drain_ms: 5_000,
            flush_ms: 5_000,
        }
    }
}

/// A running gateway, its data directory and a consumer of
/// `ExchangeCaptured` subscribed before any traffic.
pub struct TestGateway {
    pub running: Running,
    pub observer: MpscSubscription,
    pub data_dir: PathBuf,
    pub dir: TempDir,
}

pub fn config_json(upstream_base: &str, data_dir: &Path, options: Options) -> String {
    serde_json::json!({
        "ingress": {
            "listen": "127.0.0.1:0",
            "routes": [{
                "name": "anthropic",
                "prefix": PREFIX,
                "upstream": {
                    "id": "anthropic",
                    "kind": {"type": "vendor_api", "data": {"type": "anthropic"}},
                    "base_url": upstream_base,
                },
            }],
            "secrets": {"current": {"version": 1, "env": SECRET_ENV}},
            "capture": {"channel_capacity": 64},
        },
        "ops": {"listen": "127.0.0.1:0"},
        "blobs": {"root": data_dir.join("blobs")},
        "shutdown": {"drain_timeout_ms": options.drain_ms, "flush_timeout_ms": options.flush_ms},
    })
    .to_string()
}

pub fn lookup(name: &str) -> Option<String> {
    (name == SECRET_ENV).then(|| SECRET_HEX.to_owned())
}

/// Start a gateway (role `all`) routing [`PREFIX`] to `upstream_base`.
pub async fn start(upstream_base: &str, options: Options) -> TestGateway {
    let dir = tempfile::tempdir().expect("a temp dir");
    let data_dir = dir.path().join("data");
    let config = GatewayConfig::from_json(&config_json(upstream_base, &data_dir, options))
        .expect("the test config parses");
    let running = gateway::start(&config, Role::All, lookup, Arc::new(SystemClock))
        .await
        .expect("the gateway starts");
    let observer = running
        .bus()
        .subscribe(
            &[Subject::ExchangeCaptured],
            ConsumerGroup("e2e-observer".to_owned()),
            BusConfig::default().retry,
        )
        .await
        .expect("the observer subscribes");
    TestGateway {
        running,
        observer,
        data_dir,
        dir,
    }
}

impl TestGateway {
    pub fn proxy_addr(&self) -> SocketAddr {
        self.running.proxy_addr().expect("role all runs the proxy")
    }

    /// A harness whose base URL is the gateway's route.
    pub fn client(&self) -> HarnessClient {
        HarnessClient::new(self.proxy_addr()).prefix(PREFIX)
    }

    pub fn log_path(&self) -> PathBuf {
        exchange_log_path(&self.data_dir)
    }

    /// The next captured exchange's envelope, acked, waiting at most
    /// `wait`.
    pub async fn next_captured(&mut self, wait: Duration) -> Option<Envelope> {
        let delivery = tokio::time::timeout(wait, self.observer.next())
            .await
            .ok()
            .flatten()?
            .expect("a decodable delivery");
        self.observer
            .ack(delivery.id)
            .await
            .expect("the observer acks");
        Some(delivery.envelope)
    }

    /// Wait until the capture stage has handled `count` exchanges.
    pub async fn settle(&self, count: u64) {
        for _ in 0..500 {
            let pipeline = self.running.health().pipeline;
            if pipeline.published + pipeline.normalize_failed + pipeline.store_failed >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The exchange inside an `ExchangeCaptured` envelope.
pub fn exchange(envelope: &Envelope) -> &Exchange {
    match &envelope.event {
        BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) => exchange,
        other => panic!("not ExchangeCaptured: {other:?}"),
    }
}

/// Every hash the exchange names: the request, then the response or
/// partial response.
pub fn referenced(exchange: &Exchange) -> Vec<MessageHash> {
    let mut hashes = exchange.request.clone();
    match &exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => hashes.push(*response),
        ExchangeOutcome::Failed {
            partial_response, ..
        } => hashes.extend(partial_response.iter().copied()),
    }
    hashes
}

pub fn cases() -> Vec<Case> {
    anthropic::cases().expect("the corpus loads")
}

pub fn generation_cases() -> Vec<Case> {
    cases()
        .into_iter()
        .filter(|case| matches!(case.meta.endpoint, Endpoint::Generation { .. }))
        .collect()
}

pub fn case(name: &str) -> Case {
    anthropic::case(name).expect("the case loads")
}

/// L1's golden normalization of a corpus case
/// (`crates/canonical/tests/golden/anthropic/<case>.json`).
pub fn golden(case: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../canonical/tests/golden/anthropic")
        .join(format!("{case}.json"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));
    serde_json::from_str(&text).expect("a golden is JSON")
}

/// The parts of an exchange that do not depend on when, by whom or under
/// which ids it was captured: continuation, request hashes, the outcome
/// without its times, and the protocol, model and transport.
pub fn comparable(exchange: &Value) -> Value {
    let mut outcome = exchange["outcome"].clone();
    if let Some(data) = outcome.get_mut("data").and_then(Value::as_object_mut) {
        for time in ["first_chunk_at", "finished_at", "failed_at"] {
            data.remove(time);
        }
    }
    serde_json::json!({
        "continuation": exchange["continuation"],
        "request": exchange["request"],
        "outcome": outcome,
        "protocol": exchange["meta"]["protocol"],
        "model": exchange["meta"]["model"],
        "transport": exchange["meta"]["transport"],
    })
}

/// A GET with no headers or body, for the ops listener.
pub fn get(target: &str) -> CorpusRequest {
    CorpusRequest {
        method: Method::GET,
        target: PathAndQuery::try_from(target).expect("a target"),
        headers: Headers::new(),
        body: Bytes::new(),
    }
}

/// GET `target` on `addr`.
pub async fn ops_get(addr: SocketAddr, target: &str) -> CollectedResponse {
    HarnessClient::new(addr)
        .send(&get(target))
        .await
        .expect("the ops listener answers")
}
