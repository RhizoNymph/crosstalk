//! A Postgres-mode gateway (a `store` section) whose database never
//! answers: the proxy forwards every request unchanged
//! (`ingress.proxy.forwarding-independent-of-capture-store`, INV-1221),
//! capture spools to the data volume, `/readyz` stays 200 with `status:
//! degraded` and `capture: spooling`, and a full spool drops captures,
//! counted `spool_full`, while the proxy keeps answering and `/readyz`
//! turns 503. No database is needed: `DATABASE_URL` names a closed port.
//! Evidence paths: `crosstalk_gateway::postgres_down::*`.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_gateway::config::GatewayConfig;
use crosstalk_gateway::gateway::{self, Running};
use crosstalk_gateway::role::Role;
use crosstalk_spec::support::SystemClock;
use crosstalk_testkit::client::{BodyEnd, CollectedResponse, HarnessClient};
use crosstalk_testkit::corpus::{Case, CorpusRequest, Endpoint, Headers, anthropic};
use crosstalk_testkit::upstream::{FakeUpstream, Reply, Script};
use hyper::http::uri::PathAndQuery;
use hyper::{Method, StatusCode};
use serde_json::Value;

const PREFIX: &str = "/anthropic";
const SECRET_ENV: &str = "CROSSTALK_TEST_SECRET_V1";
const SECRET_HEX: &str = "5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2";
/// Nothing listens on port 1: every connection is refused at once.
const UNREACHABLE: &str = "postgres://crosstalk@127.0.0.1:1/crosstalk";

fn lookup(name: &str) -> Option<String> {
    match name {
        SECRET_ENV => Some(SECRET_HEX.to_owned()),
        "DATABASE_URL" => Some(UNREACHABLE.to_owned()),
        _ => None,
    }
}

fn config(upstream_base: &str, data_dir: &Path, spool: Value) -> GatewayConfig {
    let text = serde_json::json!({
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
        "store": {
            "pool": {"max_connections": 4, "acquire_timeout_ms": 200},
            "bus": {"publish_timeout_micros": 300_000},
        },
        "spool": spool,
        "blobs": {"root": data_dir.join("blobs")},
        "shutdown": {"drain_timeout_ms": 2_000, "flush_timeout_ms": 2_000},
    })
    .to_string();
    GatewayConfig::from_json(&text).expect("the test config parses")
}

async fn start(upstream: &FakeUpstream, data_dir: &Path, spool: Value) -> Running {
    gateway::start(
        &config(&upstream.base_url(), data_dir, spool),
        Role::All,
        lookup,
        Arc::new(SystemClock),
    )
    .await
    .expect("the gateway starts without its database")
}

fn generation_cases() -> Vec<Case> {
    anthropic::cases()
        .expect("the corpus loads")
        .into_iter()
        .filter(|case| matches!(case.meta.endpoint, Endpoint::Generation { .. }))
        .collect()
}

async fn ops_get(running: &Running, target: &str) -> CollectedResponse {
    let request = CorpusRequest {
        method: Method::GET,
        target: PathAndQuery::try_from(target).expect("a target"),
        headers: Headers::new(),
        body: Bytes::new(),
    };
    HarnessClient::new(running.ops_addr())
        .send(&request)
        .await
        .expect("the ops listener answers")
}

async fn json(running: &Running, target: &str) -> (StatusCode, Value) {
    let response = ops_get(running, target).await;
    let value = serde_json::from_slice(&response.body).expect("JSON");
    (response.status, value)
}

/// Send every generation case through the proxy; each response must be
/// the upstream's, unchanged. Returns how many were sent.
async fn forward_every_case(running: &Running, upstream: &FakeUpstream) -> u64 {
    let client = HarnessClient::new(running.proxy_addr().expect("role all runs the proxy"))
        .prefix(PREFIX);
    let mut sent = 0;
    for case in generation_cases() {
        upstream
            .reply_next(Reply::from_case(&case))
            .await
            .expect("scripted");
        let response = client.send(&case.request).await.expect("the gateway answers");
        assert_eq!(response.end, BodyEnd::Complete, "case {}", case.name);
        assert_eq!(
            response.differences_from(&case.response),
            Vec::new(),
            "case {}: the client's response changed",
            case.name
        );
        let received = upstream.received().await.expect("the upstream log");
        let last = received.last().expect("the upstream got the request");
        assert_eq!(
            last.differences_from(&case.request),
            Vec::new(),
            "case {}: the upstream's request changed",
            case.name
        );
        sent += 1;
    }
    sent
}

/// Wait until the capture stage finished `count` exchanges, one way or
/// another.
async fn captured(running: &Running, count: u64) {
    for _ in 0..1_000 {
        let pipeline = running.health().pipeline;
        let spool_full = running.health().spool.map_or(0, |spool| spool.capture_spool_full);
        if pipeline.published + pipeline.normalize_failed + pipeline.store_failed
            + pipeline.publish_failed
            + spool_full
            >= count
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the capture stage did not finish {count} exchanges");
}

/// `ingress.proxy.forwarding-independent-of-capture-store` (INV-1221):
/// through a database outage, then with a spool too small for anything,
/// the proxy forwards and relays every request unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_forwards_through_outage_and_full_spool() {
    capture_spools_while_the_database_is_down().await;
    a_full_spool_drops_counted_and_the_proxy_still_answers().await;
}

/// The spool's readiness: the database never answers, the proxy forwards
/// every case unchanged, every capture is spooled (its publish returned
/// `Ok`), and the process stays ready, degraded.
async fn capture_spools_while_the_database_is_down() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let dir = tempfile::tempdir().expect("a temp dir");
    let data_dir = dir.path().join("data");
    let running = start(&upstream, &data_dir, serde_json::json!({})).await;

    let sent = forward_every_case(&running, &upstream).await;
    assert!(sent > 0);
    captured(&running, sent).await;
    let health = running.health();
    assert_eq!(health.pipeline.published, sent, "every capture's publish returned Ok");
    let spool = health.spool.expect("a spool section in postgres mode");
    assert_eq!(spool.state, "spooling");
    assert_eq!(spool.records, sent);
    assert_eq!(spool.appended, sent);
    assert!(spool.oldest_at_micros.is_some());
    assert!(data_dir.join("spool").join("LOCK").exists());

    let (status, ready) = json(&running, "/readyz").await;
    assert_eq!(status, StatusCode::OK, "{ready}");
    assert_eq!(ready["ready"], true);
    assert_eq!(ready["status"], "degraded");
    assert_eq!(ready["capture"], "spooling");
    assert!(
        ready["database"]
            .as_str()
            .is_some_and(|text| text.starts_with("unreachable")),
        "{ready}"
    );
    assert_eq!(ready["pipeline"], "waiting_for_database");
    assert_eq!(ready["recovery"], "pending");
    assert_eq!(ready["migrations"], "unknown");

    let (status, health) = json(&running, "/healthz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["status"], "degraded");
    assert_eq!(health["spool"]["state"], "spooling");
    assert!(health.get("bus").is_none(), "no bus section without the database");

    let metrics = ops_get(&running, "/metrics").await;
    let text = String::from_utf8(metrics.body.to_vec()).expect("text");
    for line in [
        "crosstalk_spool_state{state=\"spooling\"} 1".to_owned(),
        "crosstalk_spool_state{state=\"direct\"} 0".to_owned(),
        format!("crosstalk_spool_records {sent}"),
        format!("crosstalk_spool_appended_total {sent}"),
        "crosstalk_capture_uncaptured_total{reason=\"spool_full\"} 0".to_owned(),
    ] {
        assert!(text.lines().any(|candidate| candidate == line), "missing {line}\n{text}");
    }

    let report = running.shutdown().await;
    assert!(report.capture_drained, "{report:?}");
}

/// A spool too small for any envelope: every capture is dropped, counted
/// `spool_full`, and still the proxy answers every request unchanged; the
/// process is not ready (`dropping: spool full`).
async fn a_full_spool_drops_counted_and_the_proxy_still_answers() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let dir = tempfile::tempdir().expect("a temp dir");
    let data_dir = dir.path().join("data");
    let running = start(
        &upstream,
        &data_dir,
        serde_json::json!({"max_bytes": 64, "segment_bytes": 64, "probe_ms": 50}),
    )
    .await;

    let sent = forward_every_case(&running, &upstream).await;
    captured(&running, sent).await;
    let health = running.health();
    let spool = health.spool.expect("a spool section");
    assert_eq!(spool.capture_spool_full, sent, "{spool:?}");
    assert_eq!(spool.records, 0);
    assert_eq!(health.pipeline.published, 0);
    assert_eq!(health.capture.captured, sent);

    let (status, ready) = json(&running, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{ready}");
    assert_eq!(ready["ready"], false);
    assert_eq!(ready["capture"], "dropping: spool full");

    let metrics = ops_get(&running, "/metrics").await;
    let text = String::from_utf8(metrics.body.to_vec()).expect("text");
    let line = format!("crosstalk_capture_uncaptured_total{{reason=\"spool_full\"}} {sent}");
    assert!(text.lines().any(|candidate| candidate == line), "missing {line}\n{text}");
    let line = format!("crosstalk_pipeline_exchanges_total{{outcome=\"spool_full\"}} {sent}");
    assert!(text.lines().any(|candidate| candidate == line), "missing {line}\n{text}");

    running.shutdown().await;
}

/// A second process on the same data directory cannot open the spool: its
/// `LOCK` is held for the first one's lifetime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_process_on_the_data_directory_is_refused() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let dir = tempfile::tempdir().expect("a temp dir");
    let data_dir = dir.path().join("data");
    let first = start(&upstream, &data_dir, serde_json::json!({})).await;
    let second = gateway::start(
        &config(&upstream.base_url(), &data_dir, serde_json::json!({})),
        Role::All,
        lookup,
        Arc::new(SystemClock),
    )
    .await;
    assert!(
        matches!(second, Err(gateway::StartError::Spool(_))),
        "{:?}",
        second.err()
    );
    first.shutdown().await;
}
