//! End to end: the `crosstalk` gateway (role `all`) on loopback sockets,
//! between testkit's fake Claude Code client and its fake Anthropic
//! upstream, with the corpus. Evidence paths: `crosstalk_gateway::e2e::*`.

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use crosstalk_gateway::inspect;
use crosstalk_gateway::log;
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::observed::exchange::{ExchangeFailure, ExchangeOutcome};
use crosstalk_spec::observed::message::{Role, encoding};
use crosstalk_testkit::client::{BodyEnd, HarnessClient, Next};
use crosstalk_testkit::upstream::{FakeUpstream, Fault, Pacing, Reply, Script};
use crosstalk_transport::blob::FsBlobStore;
use hyper::StatusCode;
use serde_json::Value;

use support::{
    Options, case, comparable, exchange, generation_cases, golden, ops_get, referenced, start,
};

const WAIT: Duration = Duration::from_secs(5);

/// The golden's message bodies, each checked against the blob store: the
/// bytes are there, hash to their key, decode as a canonical body, and are
/// the golden body.
async fn check_bodies(blobs: &FsBlobStore, golden: &Value, case: &str) {
    for message in golden["messages"].as_array().expect("messages") {
        let hash: MessageHash = serde_json::from_value(message["hash"].clone()).expect("a hash");
        let bytes = blobs
            .get(hash)
            .await
            .expect("the blob store reads")
            .unwrap_or_else(|| panic!("case {case}: body {} missing", message["hash"]));
        assert_eq!(encoding::hash_bytes(&bytes), hash, "case {case}");
        encoding::decode(&bytes).expect("a canonical body");
        let body: Value = serde_json::from_slice(&bytes).expect("JSON");
        assert_eq!(
            body, message["body"],
            "case {case}: body differs from the golden"
        );
    }
    for media in golden["media"].as_array().expect("media") {
        let hash: MessageHash = serde_json::from_value(media["hash"].clone()).expect("a hash");
        assert!(
            blobs.get(hash).await.expect("reads").is_some(),
            "case {case}: media missing"
        );
    }
}

/// Every generation case through the running gateway: the client gets the
/// recorded response unchanged, the upstream the recorded request
/// unchanged, exactly one `ExchangeCaptured` carries L1's normalization of
/// it (the golden), its bodies are in the blob store, and the exchange log
/// holds exactly the published envelopes after shutdown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generation_cases_pass_through_unchanged_and_are_captured_once() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = start(&upstream.base_url(), Options::default()).await;
    let client = gateway.client();
    let mut published = Vec::new();
    for case in generation_cases() {
        upstream
            .reply_next(Reply::from_case(&case))
            .await
            .expect("scripted");
        let response = client
            .send(&case.request)
            .await
            .expect("the gateway answers");
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

        let envelope = gateway
            .next_captured(WAIT)
            .await
            .unwrap_or_else(|| panic!("case {}: no ExchangeCaptured", case.name));
        let captured = exchange(&envelope);
        let golden = golden(&case.name);
        let observed = serde_json::to_value(captured).expect("encodes");
        assert_eq!(
            comparable(&observed),
            comparable(&golden["exchange"]),
            "case {}: the captured exchange is not L1's normalization",
            case.name
        );
        let client_meta = &captured.meta.client;
        assert_eq!(client_meta.harness.as_ref(), Some(&case.meta.harness.claim));
        assert_eq!(client_meta.ids, case.meta.harness.ids);
        assert_eq!(client_meta.class, case.meta.harness.class);
        assert_eq!(
            client_meta
                .credential
                .as_ref()
                .map(|credential| credential.scheme),
            case.meta
                .credential
                .as_ref()
                .map(|credential| credential.scheme)
        );
        check_bodies(gateway.running.blobs(), &golden, &case.name).await;
        published.push(envelope);
    }
    assert!(
        gateway
            .next_captured(Duration::from_millis(300))
            .await
            .is_none(),
        "an exchange was published twice"
    );
    let ids: BTreeSet<ExchangeId> = published
        .iter()
        .map(|envelope| exchange(envelope).meta.id)
        .collect();
    assert_eq!(ids.len(), published.len(), "one exchange per request");

    let health = gateway.running.health();
    assert_eq!(health.capture.captured, published.len() as u64);
    assert_eq!(health.pipeline.published, published.len() as u64);

    let log_path = gateway.log_path();
    let report = gateway.running.shutdown().await;
    assert!(report.capture_drained && report.log_drained, "{report:?}");
    let logged = log::read(&log_path).await.expect("the log reads");
    assert_eq!(logged.torn_tail, 0);
    let mut logged = logged.entries;
    logged.sort_by_key(|envelope| envelope.id);
    published.sort_by_key(|envelope| envelope.id);
    assert_eq!(
        logged, published,
        "the log holds exactly the published envelopes"
    );
}

/// Claude Code's system turn (`role: "system"` inside `messages`, besides
/// the top-level `system`): the client gets the reply unchanged and the
/// exchange is published, the turn a System message at its own position,
/// never counted `normalize_failed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_turn_exchange_is_published() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = start(&upstream.base_url(), Options::default()).await;
    let case = case("system_turn_streaming");
    upstream
        .reply_next(Reply::from_case(&case))
        .await
        .expect("scripted");
    let response = gateway
        .client()
        .send(&case.request)
        .await
        .expect("the gateway answers");
    assert_eq!(response.differences_from(&case.response), Vec::new());

    let envelope = gateway
        .next_captured(WAIT)
        .await
        .expect("the exchange is published");
    let captured = exchange(&envelope);
    let mut roles = Vec::new();
    for hash in &captured.request {
        let bytes = gateway
            .running
            .blobs()
            .get(*hash)
            .await
            .expect("the blob store reads")
            .expect("the body is stored");
        roles.push(encoding::decode(&bytes).expect("a canonical body").role());
    }
    assert_eq!(
        roles,
        [Role::System, Role::User, Role::System],
        "the system prompt, the user turn, then the system turn in place"
    );
    gateway.settle(1).await;
    let health = gateway.running.health();
    assert_eq!(health.capture.captured, 1);
    assert_eq!(health.pipeline.published, 1);
    assert_eq!(health.pipeline.normalize_failed, 0);
    gateway.running.shutdown().await;
}

/// `canonical.capture.blobs-before-event` (integration): a consumer on the
/// gateway's bus, reading the gateway's blob store through its own handle
/// on the same root, finds every body each `ExchangeCaptured` references
/// the moment the event arrives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_reads_every_referenced_blob() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = start(&upstream.base_url(), Options::default()).await;
    let reader = FsBlobStore::open(gateway.running.blobs().root())
        .await
        .expect("a second handle on the blob root");
    let client = gateway.client();
    let cases = generation_cases();
    for case in &cases {
        upstream
            .reply_next(Reply::from_case(case))
            .await
            .expect("scripted");
        let _ = client
            .send(&case.request)
            .await
            .expect("the gateway answers");
    }
    for _ in &cases {
        let envelope = gateway
            .next_captured(WAIT)
            .await
            .expect("an ExchangeCaptured");
        let captured = exchange(&envelope);
        for hash in referenced(captured) {
            let bytes = reader
                .get(hash)
                .await
                .expect("the blob store reads")
                .unwrap_or_else(|| {
                    panic!(
                        "exchange {}: body {} not stored before the event",
                        captured.meta.id.ulid_text(),
                        hash.digest().to_hex()
                    )
                });
            assert_eq!(encoding::hash_bytes(&bytes), hash);
            encoding::decode(&bytes).expect("a canonical body");
        }
    }
    gateway.running.shutdown().await;
}

/// Token counting, model listing and the probe are forwarded both ways
/// unchanged and never captured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_generation_routes_are_forwarded_and_not_captured() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = start(&upstream.base_url(), Options::default()).await;
    let client = gateway.client();
    for name in ["count_tokens", "models_list", "hello_probe"] {
        let case = case(name);
        upstream
            .reply_next(Reply::from_case(&case))
            .await
            .expect("scripted");
        let response = client
            .send(&case.request)
            .await
            .expect("the gateway answers");
        assert_eq!(
            response.differences_from(&case.response),
            Vec::new(),
            "{name}"
        );
        let received = upstream.received().await.expect("the upstream log");
        let last = received.last().expect("the upstream got the request");
        assert_eq!(last.differences_from(&case.request), Vec::new(), "{name}");
    }
    assert!(
        gateway
            .next_captured(Duration::from_millis(300))
            .await
            .is_none()
    );
    let health = gateway.running.health();
    assert_eq!(health.capture.captured, 0);
    assert_eq!(health.capture.unclassified, 0);
    assert_eq!(health.pipeline.published, 0);
    let log_path = gateway.log_path();
    gateway.running.shutdown().await;
    assert!(
        log::read(&log_path)
            .await
            .expect("reads")
            .entries
            .is_empty()
    );
}

/// An upstream error status reaches the client as the upstream sent it and
/// is captured as `Failed(Upstream { status })` with its request; an
/// unreachable upstream is answered 502 by the gateway and captured as
/// `Failed(UpstreamUnreachable)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_errors_are_relayed_and_captured_as_failed() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = start(&upstream.base_url(), Options::default()).await;
    let case = case("text_turn_streaming");
    let reply = Reply::error(StatusCode::INTERNAL_SERVER_ERROR, "upstream exploded");
    upstream.reply_next(reply.clone()).await.expect("scripted");
    let response = gateway
        .client()
        .send(&case.request)
        .await
        .expect("the gateway answers");
    assert_eq!(response.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(response.body, reply.body());
    let envelope = gateway.next_captured(WAIT).await.expect("captured");
    let captured = exchange(&envelope);
    assert!(
        matches!(
            captured.outcome,
            ExchangeOutcome::Failed {
                failure: ExchangeFailure::Upstream { status: 500 },
                partial_response: None,
                ..
            }
        ),
        "{:?}",
        captured.outcome
    );
    let golden = golden(&case.name);
    assert_eq!(
        captured.request.len(),
        golden["exchange"]["request"].as_array().map_or(0, Vec::len)
    );
    for hash in &captured.request {
        assert!(
            gateway
                .running
                .blobs()
                .get(*hash)
                .await
                .expect("reads")
                .is_some()
        );
    }
    gateway.running.shutdown().await;

    // A port nothing listens on.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = closed.local_addr().expect("address");
    drop(closed);
    let mut gateway = start(&format!("http://{address}"), Options::default()).await;
    let response = gateway
        .client()
        .send(&case.request)
        .await
        .expect("the gateway answers");
    assert_eq!(response.status, StatusCode::BAD_GATEWAY);
    let envelope = gateway.next_captured(WAIT).await.expect("captured");
    assert!(matches!(
        exchange(&envelope).outcome,
        ExchangeOutcome::Failed {
            failure: ExchangeFailure::UpstreamUnreachable,
            ..
        }
    ));
    gateway.running.shutdown().await;
}

/// Shutdown during a stream: the listener closes at once, the in-flight
/// response runs to its end unchanged, and its exchange is captured and
/// logged before the gateway stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_stream_finishes_during_shutdown() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let gateway = start(&upstream.base_url(), Options::default()).await;
    let case = case("text_turn_streaming");
    upstream
        .reply_next(Reply::from_case(&case).paced(Pacing::every(Duration::from_millis(60))))
        .await
        .expect("scripted");
    let proxy = gateway.proxy_addr();
    let mut stream = gateway
        .client()
        .open(&case.request)
        .await
        .expect("a response head");
    let first = stream.next().await;
    assert!(matches!(first, Next::Chunk(_)), "{first:?}");

    let log_path = gateway.log_path();
    let support::TestGateway {
        running,
        observer,
        dir: _dir,
        ..
    } = gateway;
    let shutdown = tokio::spawn(running.shutdown());
    // The listener closes promptly; a new connection is refused.
    let mut refused = false;
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(proxy).await.is_err() {
            refused = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(refused, "the proxy kept accepting after shutdown began");

    let Next::Chunk(first) = first else {
        unreachable!("checked above")
    };
    let rest = stream.collect().await;
    assert_eq!(rest.end, BodyEnd::Complete);
    let mut body = first.bytes.to_vec();
    body.extend_from_slice(&rest.body);
    assert_eq!(body, case.response_bytes().to_vec(), "the stream changed");

    let report = shutdown.await.expect("shutdown completes");
    assert_eq!(report.proxy.cut, 0, "{report:?}");
    assert!(report.capture_drained && report.log_drained, "{report:?}");
    drop(observer);
    let logged = log::read(&log_path).await.expect("reads").entries;
    assert_eq!(logged.len(), 1);
    assert!(matches!(
        exchange(&logged[0]).outcome,
        ExchangeOutcome::Completed { .. }
    ));
}

/// Shutdown during a stalled stream: the drain deadline passes, the
/// connection is closed, the client's body ends aborted, and the exchange is
/// still captured (as `client_disconnected`, with what arrived) and logged.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_stream_is_cut_at_the_drain_deadline_and_captured() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let gateway = start(
        &upstream.base_url(),
        Options {
            drain_ms: 300,
            flush_ms: 5_000,
        },
    )
    .await;
    let case = case("text_turn_streaming");
    upstream
        .reply_next(Reply::from_case(&case).with_fault(Fault::Stall { after_chunks: 2 }))
        .await
        .expect("scripted");
    let mut stream = gateway
        .client()
        .open(&case.request)
        .await
        .expect("a response head");
    for _ in 0..2 {
        assert!(matches!(stream.next().await, Next::Chunk(_)));
    }
    let log_path = gateway.log_path();
    let blobs = gateway.running.blobs().clone();
    let report = gateway.running.shutdown().await;
    assert_eq!(report.proxy.cut, 1, "{report:?}");
    assert!(report.capture_drained && report.log_drained, "{report:?}");
    assert!(
        matches!(stream.next().await, Next::End(BodyEnd::Aborted { .. })),
        "the client's stream was not cut"
    );
    let logged = log::read(&log_path).await.expect("reads").entries;
    assert_eq!(logged.len(), 1);
    let captured = exchange(&logged[0]);
    let ExchangeOutcome::Failed {
        failure,
        partial_response,
        ..
    } = &captured.outcome
    else {
        panic!("a cut stream completed: {:?}", captured.outcome);
    };
    assert_eq!(*failure, ExchangeFailure::ClientDisconnected);
    for hash in captured.request.iter().chain(partial_response.iter()) {
        assert!(blobs.get(*hash).await.expect("reads").is_some());
    }
}

/// The ops listener: `/healthz` with the counters, `/readyz`, `/metrics`,
/// the `healthcheck` command against it, and `inspect` over what was
/// captured.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ops_endpoints_and_inspect_report_the_capture() {
    let upstream = FakeUpstream::start(Script::new()).await.expect("upstream");
    let mut gateway = start(&upstream.base_url(), Options::default()).await;
    let case = case("tool_use_streaming");
    upstream
        .reply_next(Reply::from_case(&case))
        .await
        .expect("scripted");
    let _ = gateway
        .client()
        .send(&case.request)
        .await
        .expect("answered");
    let envelope = gateway.next_captured(WAIT).await.expect("captured");
    let id = exchange(&envelope).meta.id.ulid_text();
    gateway.settle(1).await;
    let ops = gateway.running.ops_addr();

    let health = ops_get(ops, "/healthz").await;
    assert_eq!(health.status, StatusCode::OK);
    let report: Value = serde_json::from_slice(&health.body).expect("JSON");
    assert_eq!(report["status"], "ok");
    assert_eq!(report["capture"]["captured"], 1);
    assert_eq!(report["pipeline"]["published"], 1);
    assert!(
        report["live"]["stages"]["l3-reconstruct"].is_u64(),
        "{report}"
    );

    let ready = ops_get(ops, "/readyz").await;
    assert_eq!(ready.status, StatusCode::OK, "{:?}", ready.body);
    let readiness: Value = serde_json::from_slice(&ready.body).expect("JSON");
    assert_eq!(readiness["ready"], true);
    assert_eq!(readiness["database"], "not_configured");
    let tasks: Vec<&str> = readiness["tasks"]
        .as_array()
        .expect("tasks")
        .iter()
        .filter_map(|task| task["name"].as_str())
        .collect();
    // The test config has no api section, so no `api` task.
    assert_eq!(tasks, ["exchange_log", "capture", "live", "proxy"]);

    let metrics = ops_get(ops, "/metrics").await;
    assert_eq!(metrics.status, StatusCode::OK);
    let text = String::from_utf8(metrics.body.to_vec()).expect("text");
    assert!(
        text.lines()
            .any(|line| line == "crosstalk_capture_exchanges_total 1")
    );
    assert!(
        text.lines()
            .any(|line| line == "crosstalk_pipeline_exchanges_total{outcome=\"published\"} 1")
    );
    assert_eq!(ops_get(ops, "/nothing").await.status, StatusCode::NOT_FOUND);

    let url = format!("http://{ops}/readyz");
    assert!(crosstalk_gateway::healthcheck::check(&url).await.is_ok());
    assert!(
        crosstalk_gateway::healthcheck::check(&format!("http://{ops}/nothing"))
            .await
            .is_err()
    );

    // Wait for the log, then read it back as `crosstalk inspect` does.
    for _ in 0..200 {
        if gateway.running.health().log.written >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let config = crosstalk_gateway::config::GatewayConfig::from_json(&support::config_json(
        &upstream.base_url(),
        &gateway.data_dir,
        Options::default(),
    ))
    .expect("parses");
    let listing = inspect::list(&config).await.expect("lists");
    assert!(listing.contains(&id), "{listing}");
    assert!(listing.contains("completed(tool_use)"), "{listing}");
    let shown = inspect::show(&config, &id).await.expect("shows");
    let shown: Value = serde_json::from_str(&shown).expect("JSON");
    let messages = shown["messages"].as_array().expect("messages");
    assert_eq!(
        messages.last().map(|message| &message["role"]),
        Some(&Value::from("response"))
    );
    assert!(
        messages
            .iter()
            .all(|message| message["body"]["type"].is_string())
    );
    assert!(matches!(
        inspect::show(&config, "01M3TC5H00000001R000000003").await,
        Err(inspect::InspectError::UnknownExchange(_))
    ));

    let report = gateway.running.shutdown().await;
    assert!(report.log_drained);
    let draining_client = HarnessClient::new(ops);
    assert!(
        draining_client
            .send(&support::get("/healthz"))
            .await
            .is_err(),
        "the ops listener stops last"
    );
}
