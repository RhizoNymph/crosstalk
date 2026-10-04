//! Latency budgets (`bench` evidence), as timing tests that assert their
//! threshold. They measure wall time on a multi-threaded runtime, so they
//! are `#[ignore]`d in the default run and meant for release builds:
//!
//! ```sh
//! cargo test --release -p crosstalk-ingress -- --ignored --nocapture --test-threads=1 benches
//! ```
//!
//! Each compares the same proxy with and without capture on the measured
//! path, through in-memory connections, so only the proxy's own work
//! differs: a generation request (`POST /v1/messages`, teed, framed,
//! decoded) against the same request on a route the adapter classifies
//! `Other` (`POST /v1/messages/batches`, relayed plainly).

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crosstalk_spec::ids::{SeededRandom, UlidGenerator};
use crosstalk_spec::interfaces::l0_ingress::ContentEncoding;
use crosstalk_spec::support::SystemClock;
use crosstalk_testkit::corpus::CorpusRequest;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::adapter::AnthropicAdapter;
use crate::capture::CaptureSender;
use crate::config::LimitsConfig;
use crate::decode::AdapterDecoder;
use crate::proxy::{Proxy, ProxyParts};
use crate::routing::Routes;
use crate::tests::sim_support::{self, Feed, Manual, Read, SimConnector, SimReply, open};
use crate::tests::support::{
    adapter, anthropic_api, case, identifier, limits_with, non_zero, route,
};

/// The budget both invariants state.
const BUDGET: Duration = Duration::from_millis(1);

/// Iterations run before measuring (connection set-up, allocator warm-up).
const WARM_UP: usize = 5;

/// Measurement rounds; the added p99 is their median.
const ROUNDS: usize = 9;

type BenchProxy = Proxy<AnthropicAdapter, AdapterDecoder<AnthropicAdapter>, SimConnector>;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a runtime")
}

fn proxy(
    connector: SimConnector,
    limits: LimitsConfig,
) -> (
    BenchProxy,
    mpsc::Receiver<crosstalk_spec::interfaces::l0_ingress::RawExchange>,
) {
    let routes = Routes::new(&[route("http://upstream.bench", anthropic_api())]).expect("route");
    let (sender, captured) = mpsc::channel(1 << 16);
    let adapter = adapter(&limits);
    let proxy = Proxy::new(ProxyParts {
        routes,
        identifier: identifier(),
        decoder: AdapterDecoder::new(Arc::clone(&adapter), limits.decoded_bytes),
        adapter,
        connector,
        capture: CaptureSender::new(sender),
        clock: Arc::new(SystemClock),
        ids: UlidGenerator::new(Arc::new(SystemClock), SeededRandom::new(5)),
        limits,
        observer: None,
    });
    (proxy, captured)
}

/// The same request on a route that is not captured.
fn uncaptured(request: &CorpusRequest) -> CorpusRequest {
    let mut request = request.clone();
    request.target = "/v1/messages/batches".parse().expect("a target");
    request
}

fn percentile(samples: &mut [Duration], percent: usize) -> Duration {
    samples.sort_unstable();
    let index = (samples.len() * percent).div_ceil(100).saturating_sub(1);
    samples.get(index).copied().unwrap_or_default()
}

/// The p99 one variant adds over the other, robust to a loaded machine:
/// each round's p99 difference, then the median over rounds (negative
/// differences count as zero added).
fn added_p99(rounds: &mut [(Vec<Duration>, Vec<Duration>)]) -> Duration {
    let mut differences: Vec<i128> = rounds
        .iter_mut()
        .map(|(with, without)| {
            percentile(with, 99).as_nanos() as i128 - percentile(without, 99).as_nanos() as i128
        })
        .collect();
    differences.sort_unstable();
    let median = differences
        .get(differences.len() / 2)
        .copied()
        .unwrap_or_default();
    Duration::from_nanos(u64::try_from(median.max(0)).unwrap_or(u64::MAX))
}

/// Per-chunk relay latency through one streamed response: each chunk is
/// sent upstream only after the client got the previous one, and timed
/// from the upstream's send to the client's read.
async fn relay_latencies(
    proxy: &BenchProxy,
    upstream: &sim_support::SimUpstream,
    request: &CorpusRequest,
    chunks: &[Bytes],
) -> Vec<Duration> {
    let (manual, feed) = Manual::event_stream();
    upstream.push(SimReply::Manual(manual));
    let mut response = open(proxy, request).await.expect("a response head");
    let mut latencies = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let sent = Instant::now();
        let _ = feed.send(Feed::Data(chunk.clone()));
        let mut got = 0;
        while got < chunk.len() {
            match response.next().await {
                Read::Chunk { bytes, .. } => got += bytes.len(),
                other => panic!("the stream ended early: {other:?}"),
            }
        }
        latencies.push(sent.elapsed());
    }
    drop(feed);
    let _ = response.collect().await;
    latencies
}

/// `ingress.capture.added-latency-bound`: the framer and the tee add at
/// most 1 ms at p99 to relaying an upstream chunk.
#[test]
#[ignore = "timing; run in release with --ignored"]
fn capture_overhead_per_chunk_p99() {
    let runtime = runtime();
    let streaming = case("text_turn_streaming");
    let frames = streaming.response.body.chunks();
    let delta = frames
        .iter()
        .find(|frame| frame.starts_with(b"event: content_block_delta"))
        .cloned()
        .expect("a delta frame");
    let mut chunks = vec![frames[0].clone()];
    chunks.extend(std::iter::repeat_n(delta, 500));
    chunks.push(frames[frames.len() - 1].clone());
    let mut rounds = runtime.block_on(async {
        let (upstream, connector) = sim_support::upstream(Vec::new());
        let (proxy, _captured) = proxy(connector, LimitsConfig::default());
        let other = uncaptured(&streaming.request);
        let mut rounds = Vec::new();
        for round in 0..ROUNDS + WARM_UP {
            let with = relay_latencies(&proxy, &upstream, &streaming.request, &chunks).await;
            let without = relay_latencies(&proxy, &upstream, &other, &chunks).await;
            if round >= WARM_UP {
                rounds.push((with, without));
            }
        }
        rounds
    });
    let added = added_p99(&mut rounds);
    let (mut with, mut without): (Vec<Duration>, Vec<Duration>) = rounds
        .iter()
        .flat_map(|(with, without)| with.iter().copied().zip(without.iter().copied()))
        .unzip();
    println!(
        "relay p50 {:?} with capture, {:?} without; pooled p99 {:?} with, {:?} without; added p99 (median of {ROUNDS} rounds) {added:?} over {} chunks",
        percentile(&mut with, 50),
        percentile(&mut without, 50),
        percentile(&mut with, 99),
        percentile(&mut without, 99),
        with.len()
    );
    assert!(added <= BUDGET, "capture adds {added:?} at p99");
}

/// When the upstream saw the first and last bytes of one request body.
#[derive(Debug)]
struct Arrival {
    first: Instant,
    last: Instant,
}

/// An upstream that times each request body's frames and answers 200.
fn timing_upstream(
    mut pipes: mpsc::UnboundedReceiver<tokio::io::DuplexStream>,
) -> mpsc::UnboundedReceiver<Arrival> {
    let (arrivals, received) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(pipe) = pipes.recv().await {
            let arrivals = arrivals.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                    let arrivals = arrivals.clone();
                    async move {
                        let mut body = request.into_body();
                        let mut first = None;
                        let mut last = Instant::now();
                        while let Some(Ok(frame)) = body.frame().await {
                            if frame.is_data() {
                                let now = Instant::now();
                                first.get_or_insert(now);
                                last = now;
                            }
                        }
                        let _ = arrivals.send(Arrival {
                            first: first.unwrap_or(last),
                            last,
                        });
                        let mut response = Response::new(Full::new(Bytes::from_static(
                            br#"{"id":"msg_1","type":"message"}"#,
                        )));
                        response.headers_mut().insert(
                            hyper::header::CONTENT_TYPE,
                            HeaderValue::from_static("application/json"),
                        );
                        Ok::<_, std::convert::Infallible>(response)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(pipe), service)
                    .await;
            });
        }
    });
    received
}

fn messages_body(decoded: usize) -> Vec<u8> {
    serde_json::json!({
        "model": "claude-opus-5-5",
        "max_tokens": 16,
        "messages": [{"role": "user", "content": "x".repeat(decoded)}],
    })
    .to_string()
    .into_bytes()
}

fn encoded(body: &[u8], encoding: ContentEncoding) -> Vec<u8> {
    match encoding {
        ContentEncoding::Identity => body.to_vec(),
        ContentEncoding::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            let _ = encoder.write_all(body);
            encoder.finish().unwrap_or_default()
        }
        ContentEncoding::Zstd => zstd::stream::encode_all(body, 3).unwrap_or_default(),
    }
}

/// `ingress.decode.no-forwarding-latency`: decoding adds at most 1 ms at
/// p99 to when the upstream gets a request's first and last body bytes,
/// for identity, gzip and zstd bodies up to the decoded-size bound.
#[test]
#[ignore = "timing; run in release with --ignored"]
fn decode_overhead_on_forwarding_p99() {
    const MAX_DECODED: usize = 4 * 1024 * 1024;
    let runtime = runtime();
    let text = case("text_turn");
    let limits = limits_with(|limits| {
        limits.decoded_bytes = non_zero(2 * MAX_DECODED as u64);
        limits.request_tee_bytes = non_zero(2 * MAX_DECODED as u64);
    });
    let mut failures = Vec::new();
    for encoding in [
        ContentEncoding::Identity,
        ContentEncoding::Gzip,
        ContentEncoding::Zstd,
    ] {
        for decoded in [64 * 1024, 1024 * 1024, MAX_DECODED] {
            let mut request = text.request.clone();
            request.body = Bytes::from(encoded(&messages_body(decoded), encoding));
            let name = match encoding {
                ContentEncoding::Identity => None,
                ContentEncoding::Gzip => Some("gzip"),
                ContentEncoding::Zstd => Some("zstd"),
            };
            if let Some(name) = name {
                request.headers.push(
                    HeaderName::from_static("content-encoding"),
                    HeaderValue::from_static(name),
                );
            }
            let other = uncaptured(&request);
            let iterations = 60;
            let mut samples = runtime.block_on(async {
                let (connector, pipes) = sim_support::connector();
                let mut arrivals = timing_upstream(pipes);
                let (proxy, mut captured) = proxy(connector, limits);
                let mut rounds = Vec::new();
                for round in 0..ROUNDS + 1 {
                    let mut samples: [Vec<(Duration, Duration)>; 2] = [Vec::new(), Vec::new()];
                    for _ in 0..iterations {
                        for (index, request) in [&request, &other].into_iter().enumerate() {
                            let started = Instant::now();
                            let response = open(&proxy, request).await.expect("a response");
                            let _ = response.collect().await;
                            let arrival = arrivals.recv().await.expect("timed");
                            if index == 0 {
                                // Let this request's decode finish before
                                // the next measurement, so runs do not
                                // overlap.
                                let _ = captured.recv().await;
                            }
                            samples[index].push((arrival.first - started, arrival.last - started));
                        }
                    }
                    // The first round warms up.
                    if round > 0 {
                        rounds.push(samples);
                    }
                }
                rounds
            });
            let unzip = |samples: &[(Duration, Duration)]| -> (Vec<Duration>, Vec<Duration>) {
                samples.iter().copied().unzip()
            };
            let mut firsts: Vec<_> = samples
                .iter()
                .map(|[with, without]| (unzip(with).0, unzip(without).0))
                .collect();
            let mut lasts: Vec<_> = samples
                .iter_mut()
                .map(|[with, without]| (unzip(with).1, unzip(without).1))
                .collect();
            let first = added_p99(&mut firsts);
            let last = added_p99(&mut lasts);
            let (mut with_last, mut without_last): (Vec<_>, Vec<_>) = lasts.into_iter().fold(
                (Vec::new(), Vec::new()),
                |(mut a, mut b), (with, without)| {
                    a.extend(with);
                    b.extend(without);
                    (a, b)
                },
            );
            println!(
                "{encoding:?} {decoded} B decoded ({} B sent): added p99 to the first byte {first:?}, to the last byte {last:?} (median of {ROUNDS} rounds); pooled last-byte p99 {:?} with decoding, {:?} without",
                request.body.len(),
                percentile(&mut with_last, 99),
                percentile(&mut without_last, 99),
            );
            if first > BUDGET || last > BUDGET {
                failures.push(format!(
                    "{encoding:?} {decoded}: first {first:?}, last {last:?}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "decoding exceeds the budget: {failures:?}"
    );
}
