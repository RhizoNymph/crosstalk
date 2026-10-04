//! Deterministic simulation tests (`dst` evidence), under `crosstalk-sim`:
//! paused time, the simulation's wall clock, in-memory connections, and
//! scenarios drawn from each seed.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use bytes::Bytes;
use crosstalk_sim::{CheckFailed, ClockStep, SimCtx, sim_test};
use crosstalk_spec::ids::ExchangeId;
use crosstalk_spec::interfaces::l0_ingress::{RawExchange, RawResponse};
use crosstalk_spec::observed::exchange::{ExchangeFailure, ExchangeStage};
use crosstalk_testkit::upstream::{Fault, Pacing, Reply, Route};
use hyper::header::{HeaderName, HeaderValue};
use tokio::time::Instant;

use super::scenario::{Scenario, run, stream_case, tagged};
use super::sim_support::{
    Feed, Manual, Read, Setup, Sim, SimReply, capture_within, drain_stages, gated, open, open_at,
    sim, sse,
};
use super::support::{case, cases, limits_with, non_zero};
use crate::capture::CaptureStats;
use crate::decode::RequestDecoder;

const SETTLE: Duration = Duration::from_secs(60);

fn check(holds: bool, message: impl FnOnce() -> String) -> Result<(), CheckFailed> {
    if holds {
        Ok(())
    } else {
        Err(CheckFailed::new(message()))
    }
}

fn accounted(stats: &CaptureStats) -> u64 {
    let counts = stats.snapshot();
    counts.captured
        + counts.decode_error
        + counts.channel_full
        + counts.channel_closed
        + counts.response_too_large
}

/// Wait (in simulated time) until `count` exchanges are accounted for.
async fn settle(stats: &CaptureStats, count: u64) -> Result<(), CheckFailed> {
    let deadline = Instant::now() + SETTLE;
    while accounted(stats) < count {
        if Instant::now() >= deadline {
            return Err(CheckFailed::new(format!(
                "only {} of {count} exchanges were accounted for: {:?}",
                accounted(stats),
                stats.snapshot()
            )));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Ok(())
}

fn drain(sim: &mut Sim<impl RequestDecoder>) -> Vec<RawExchange> {
    let mut exchanges = Vec::new();
    while let Ok(exchange) = sim.captured.try_recv() {
        exchanges.push(exchange);
    }
    exchanges
}

fn failure_of(response: &RawResponse) -> Option<ExchangeFailure> {
    match response {
        RawResponse::Complete { .. } => None,
        RawResponse::Failed { failure, .. } => Some(*failure),
    }
}

/// Run `count` drawn scenarios one after another; return them by model.
async fn run_drawn(
    ctx: &SimCtx,
    sim: &Sim<impl RequestDecoder>,
    count: usize,
) -> Result<BTreeMap<String, Scenario>, CheckFailed> {
    let case = stream_case();
    let frames = case.response.body.chunks().len();
    let mut rng = ctx.rng();
    let mut drawn = BTreeMap::new();
    for n in 0..count {
        let scenario = Scenario::draw(&mut rng, frames);
        let model = format!("model-{n}");
        ctx.step(format!("{model}: {scenario:?}"));
        run(sim, &case, scenario, &model).await?;
        drawn.insert(model, scenario);
    }
    Ok(drawn)
}

/// Stage events by exchange.
fn stages_by_exchange(
    events: Vec<crate::exchange::StageEvent>,
) -> BTreeMap<ExchangeId, Vec<ExchangeStage>> {
    let mut by = BTreeMap::<ExchangeId, Vec<ExchangeStage>>::new();
    for event in events {
        by.entry(event.exchange).or_default().push(event.stage);
    }
    by
}

sim_test! {
    /// With a capacity-one capture channel nobody drains, every client is
    /// served in full and at once; one exchange is captured and the rest
    /// are dropped and counted, never waited on.
    fn full_capture_channel_drops_without_blocking(ctx) {
        let case = stream_case();
        let sim = sim(&ctx, Setup { capacity: 1, ..Setup::default() });
        let count = 2 + ctx.rng().below(non_zero(5)) as usize;
        for n in 0..count {
            sim.upstream.push(SimReply::Scripted(Reply::from_case(&case)));
            let started = Instant::now();
            let response = open(&sim.proxy, &tagged(&case, &format!("model-{n}"))).await
                .ok_or_else(|| CheckFailed::new("no response head"))?;
            let (body, end) = response.collect().await;
            check(end == Read::End && body == *case.response_bytes(), || format!("exchange {n} was not served in full"))?;
            check(started.elapsed() == Duration::ZERO, || format!("exchange {n} took {:?}", started.elapsed()))?;
        }
        settle(&sim.stats, count as u64).await?;
        let counts = sim.stats.snapshot();
        check(counts.captured == 1 && counts.channel_full == count as u64 - 1, || format!("{counts:?}"))
    }
}

sim_test! {
    /// A framing error fails the exchange at once, but the hand-off waits
    /// for the stream's end and its body has every byte, those after the
    /// error included.
    fn handoff_waits_for_stream_end_after_frame_error(ctx) {
        let case = stream_case();
        let mut sim = sim(&ctx, Setup::default());
        let (manual, feed) = Manual::event_stream();
        sim.upstream.push(SimReply::Manual(manual));
        let mut response = open(&sim.proxy, &case.request).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let frames = case.response.body.chunks();
        let malformed = Bytes::from_static(b"event: content_block_delta\ndata: {not json\n\n");
        let after = frames[1..].to_vec();
        let mut sent = vec![frames[0].clone(), malformed.clone()];
        for chunk in [frames[0].clone(), malformed] {
            let _ = feed.send(Feed::Data(chunk));
            check(matches!(response.next().await, Read::Chunk { .. }), || "a chunk did not arrive".to_owned())?;
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
        let failed = drain_stages(&mut sim.stages).iter().any(|event| matches!(event.stage, ExchangeStage::Failed(ExchangeFailure::MalformedStream { .. })));
        check(failed, || "the malformed frame did not fail the exchange".to_owned())?;
        check(sim.captured.try_recv().is_err(), || "handed off at the error".to_owned())?;
        for chunk in after {
            let _ = feed.send(Feed::Data(chunk.clone()));
            sent.push(chunk);
            check(matches!(response.next().await, Read::Chunk { .. }), || "a chunk after the error did not arrive".to_owned())?;
            check(sim.captured.try_recv().is_err(), || "handed off before the stream ended".to_owned())?;
        }
        let last_byte = Instant::now();
        tokio::time::sleep(Duration::from_secs(5)).await;
        drop(feed);
        check(response.next().await == Read::End, || "the stream did not end".to_owned())?;
        let exchange = capture_within(&mut sim.captured, SETTLE).await.ok_or_else(|| CheckFailed::new("nothing captured"))?;
        let expected: Vec<u8> = sent.concat();
        let offset = frames[0].len() as u64;
        match &exchange.response {
            RawResponse::Failed { failure: ExchangeFailure::MalformedStream { offset: at }, partial_body } => {
                check(*at == offset, || format!("offset {at}, expected {offset}"))?;
                check(*partial_body == expected, || "the partial body is not every byte received".to_owned())?;
            }
            other => return Err(CheckFailed::new(format!("unexpected response {other:?}"))),
        }
        let held = last_byte.elapsed();
        let ended_after = exchange.ended_at.as_micros() - exchange.meta.started_at.as_micros();
        check(ended_after >= 35_000_000, || format!("ended_at is {ended_after} µs after the start; the stream ran {held:?} past the last byte"))
    }
}

sim_test! {
    /// Every forwarded exchange whose request decodes yields exactly one
    /// RawExchange, however it ends; one that does not decode yields none
    /// and is counted.
    fn one_raw_exchange_per_forwarded_exchange(ctx) {
        let mut sim = sim(&ctx, Setup::default());
        let count = 4 + ctx.rng().below(non_zero(8)) as usize;
        let drawn = run_drawn(&ctx, &sim, count).await?;
        settle(&sim.stats, count as u64).await?;
        let exchanges = drain(&mut sim);
        let mut models = BTreeMap::<String, usize>::new();
        let mut ids = BTreeSet::new();
        for exchange in &exchanges {
            *models.entry(exchange.meta.model.0.clone()).or_default() += 1;
            check(ids.insert(exchange.meta.id), || "an exchange id repeated".to_owned())?;
        }
        for (model, scenario) in &drawn {
            let got = models.get(model).copied().unwrap_or(0);
            let expected = usize::from(scenario.captured());
            check(got == expected, || format!("{model} ({scenario:?}) produced {got} RawExchanges"))?;
        }
        let bad = drawn.values().filter(|scenario| !scenario.captured()).count() as u64;
        let counts = sim.stats.snapshot();
        check(counts.decode_error == bad && exchanges.len() == drawn.len() - bad as usize, || format!("{counts:?}"))
    }
}

sim_test! {
    /// When causes race, the first one is recorded: an upstream stall
    /// against the proxy's idle timeout and the client's patience, a
    /// malformed frame before a disconnect, an error event before the end.
    /// The proxy never records UnparseableResponse.
    fn first_failure_cause_wins(ctx) {
        let case = stream_case();
        let idle = Duration::from_secs(5);
        let limits = limits_with(|limits| limits.upstream_idle_timeout_ms = Some(non_zero(5_000)));
        let mut sim = sim(&ctx, Setup { limits, ..Setup::default() });
        let mut rng = ctx.rng();
        let mut expected = BTreeMap::new();

        // A stall: whoever gives up first is the cause.
        let patience = Duration::from_millis(500 + rng.below(non_zero(9_000)));
        sim.upstream.push(SimReply::Scripted(Reply::from_case(&case).with_fault(Fault::Stall { after_chunks: 2 })));
        let mut response = open(&sim.proxy, &tagged(&case, "stall")).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let _ = response.next().await;
        let _ = response.next().await;
        let waited = tokio::time::timeout(patience, response.next()).await;
        drop(response);
        let cause = if patience < idle { ExchangeFailure::ClientDisconnected } else { ExchangeFailure::Timeout };
        if patience >= idle {
            check(matches!(waited, Ok(Read::Aborted)), || format!("the client saw {waited:?} after the idle timeout"))?;
        }
        expected.insert("stall", cause);

        // A malformed frame, then the upstream drops the connection.
        let (manual, feed) = Manual::event_stream();
        sim.upstream.push(SimReply::Manual(manual));
        let mut response = open(&sim.proxy, &tagged(&case, "malformed")).await.ok_or_else(|| CheckFailed::new("no head"))?;
        for chunk in [case.response.body.chunks()[0].clone(), Bytes::from_static(b"event: message_delta\ndata: [oops\n\n")] {
            let _ = feed.send(Feed::Data(chunk));
            let _ = response.next().await;
        }
        let _ = feed.send(Feed::Abort);
        let _ = response.collect().await;
        expected.insert("malformed", ExchangeFailure::MalformedStream { offset: case.response.body.chunks()[0].len() as u64 });

        // An error event, then a clean end.
        let overloaded = super::support::case("overloaded_mid_stream");
        sim.upstream.push(SimReply::Scripted(Reply::from_case(&overloaded)));
        let response = open(&sim.proxy, &tagged(&overloaded, "overloaded")).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let _ = response.collect().await;
        expected.insert("overloaded", ExchangeFailure::UpstreamErrorEvent);

        // A 429, then the body ends.
        let limited = super::support::case("rate_limited");
        sim.upstream.push(SimReply::Scripted(Reply::from_case(&limited)));
        let response = open(&sim.proxy, &tagged(&limited, "limited")).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let _ = response.collect().await;
        expected.insert("limited", ExchangeFailure::Upstream { status: 429 });

        settle(&sim.stats, expected.len() as u64).await?;
        for exchange in drain(&mut sim) {
            let failure = failure_of(&exchange.response);
            check(failure != Some(ExchangeFailure::UnparseableResponse), || "the proxy recorded UnparseableResponse".to_owned())?;
            let model = exchange.meta.model.0.as_str();
            let want = expected.remove(model);
            check(want.is_some() && failure == want, || format!("{model}: {failure:?}, expected {want:?}"))?;
        }
        check(expected.is_empty(), || format!("not captured: {expected:?}"))
    }
}

sim_test! {
    /// Every request the upstream receives is one client request, received
    /// once; unrouted and refused requests reach it not at all. No
    /// retries, refreshes or requests of the proxy's own.
    fn upstream_requests_map_one_to_one_to_client_requests(ctx) {
        let corpus = cases();
        let routes = corpus.iter().map(|case| (Route::of(&case.request), Reply::from_case(case))).collect();
        let mut sim = sim(&ctx, Setup { routes, ..Setup::default() });
        let mut rng = ctx.rng();
        let count = 6 + rng.below(non_zero(10)) as usize;
        let seq = HeaderName::from_static("x-test-seq");
        let mut reached = BTreeSet::new();
        for n in 0..count {
            let picked = &corpus[rng.below(non_zero(corpus.len() as u64)) as usize];
            let mut request = picked.request.clone();
            request.headers.push(seq.clone(), HeaderValue::from(n));
            match rng.below(non_zero(5)) {
                0 => {
                    let response = open_at(&sim.proxy, "", &request).await.ok_or_else(|| CheckFailed::new("no head"))?;
                    check(response.status == hyper::StatusCode::MISDIRECTED_REQUEST, || "an unrouted request was not answered 421".to_owned())?;
                    let _ = response.collect().await;
                }
                1 => {
                    sim.connector.refuse_next(1);
                    let response = open(&sim.proxy, &request).await.ok_or_else(|| CheckFailed::new("no head"))?;
                    let _ = response.collect().await;
                }
                2 => {
                    sim.upstream.push(SimReply::Scripted(Reply::from_case(picked).with_fault(Fault::Disconnect { after_chunks: 0 })));
                    let response = open(&sim.proxy, &request).await.ok_or_else(|| CheckFailed::new("no head"))?;
                    let _ = response.collect().await;
                    reached.insert(n);
                }
                _ => {
                    let response = open(&sim.proxy, &request).await.ok_or_else(|| CheckFailed::new("no head"))?;
                    let _ = response.collect().await;
                    reached.insert(n);
                }
            }
        }
        tokio::time::sleep(SETTLE).await;
        let mut seen = BTreeMap::<usize, usize>::new();
        for request in sim.upstream.drain() {
            let n: usize = request.headers.get_str("x-test-seq").and_then(|value| value.parse().ok())
                .ok_or_else(|| CheckFailed::new(format!("the upstream got a request no client sent: {} {}", request.method, request.target)))?;
            *seen.entry(n).or_default() += 1;
        }
        for (n, times) in &seen {
            check(*times == 1, || format!("request {n} reached the upstream {times} times"))?;
        }
        let seen: BTreeSet<usize> = seen.into_keys().collect();
        check(seen == reached, || format!("reached {seen:?}, expected {reached:?}"))
    }
}

sim_test! {
    /// Each upstream chunk reaches the client before the upstream sends the
    /// next: the proxy holds nothing back for a frame boundary, a whole
    /// response or capture.
    fn chunks_relayed_before_next_upstream_read(ctx) {
        let case = if ctx.rng().below(non_zero(2)) == 0 { stream_case() } else { super::support::case("thinking_streaming") };
        let sim = sim(&ctx, Setup::default());
        let (manual, feed) = Manual::event_stream();
        sim.upstream.push(SimReply::Manual(manual));
        let mut response = open(&sim.proxy, &case.request).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let mut rng = ctx.rng();
        // Split the stream at arbitrary points, not only at frames.
        let bytes = case.response_bytes().clone();
        let mut at = 0;
        while at < bytes.len() {
            let len = 1 + rng.below(non_zero(64)) as usize;
            let chunk = bytes.slice(at..(at + len).min(bytes.len()));
            at += chunk.len();
            let _ = feed.send(Feed::Data(chunk.clone()));
            let mut got = Vec::new();
            while got.len() < chunk.len() {
                match tokio::time::timeout(Duration::from_secs(1), response.next()).await {
                    Ok(Read::Chunk { bytes, .. }) => got.extend_from_slice(&bytes),
                    other => return Err(CheckFailed::new(format!("chunk at {at} not relayed before the next upstream read: {other:?}"))),
                }
            }
            check(got == chunk, || format!("chunk at {at} changed"))?;
        }
        drop(feed);
        check(response.next().await == Read::End, || "the stream did not end".to_owned())
    }
}

sim_test! {
    /// first_chunk_at is set exactly when FirstContent is reported, to that
    /// time: a paced stream or body gets the time of its first content
    /// frame; a 429, or a stream of pings cut off, gets none.
    fn first_chunk_at_set_on_first_content(ctx) {
        let mut sim = sim(&ctx, Setup::default());
        let mut rng = ctx.rng();
        let delay = Duration::from_millis(1 + rng.below(non_zero(5_000)));
        let mut expected = BTreeMap::new();
        for (name, model) in [("text_turn_streaming", "streamed"), ("text_turn", "whole")] {
            let case = case(name);
            sim.upstream.push(SimReply::Scripted(Reply::from_case(&case).paced(Pacing { first_chunk: delay, between_chunks: Duration::from_millis(3) })));
            let _ = open(&sim.proxy, &tagged(&case, model)).await.ok_or_else(|| CheckFailed::new("no head"))?.collect().await;
            expected.insert(model.to_owned(), Some(delay));
        }
        let limited = case("rate_limited");
        sim.upstream.push(SimReply::Scripted(Reply::from_case(&limited).paced(Pacing::every(delay))));
        let _ = open(&sim.proxy, &tagged(&limited, "limited")).await.ok_or_else(|| CheckFailed::new("no head"))?.collect().await;
        expected.insert("limited".to_owned(), None);
        let (manual, feed) = Manual::event_stream();
        sim.upstream.push(SimReply::Manual(manual));
        let response = open(&sim.proxy, &tagged(&stream_case(), "pings")).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let _ = feed.send(Feed::Data(sse("ping", r#"{"type": "ping"}"#)));
        let _ = feed.send(Feed::Abort);
        let _ = response.collect().await;
        expected.insert("pings".to_owned(), None);

        settle(&sim.stats, expected.len() as u64).await?;
        let responding: BTreeMap<ExchangeId, _> = drain_stages(&mut sim.stages).into_iter().filter_map(|event| match event.stage {
            ExchangeStage::Responding { first_chunk_at } => Some((event.exchange, first_chunk_at)),
            _ => None,
        }).collect();
        for exchange in drain(&mut sim) {
            let model = exchange.meta.model.0.clone();
            let since = exchange.first_chunk_at.map(|at| Duration::from_micros(at.as_micros() - exchange.meta.started_at.as_micros()));
            check(Some(&since) == expected.get(&model), || format!("{model}: first chunk after {since:?}, expected {:?}", expected.get(&model)))?;
            check(exchange.first_chunk_at == responding.get(&exchange.meta.id).copied(), || format!("{model}: first_chunk_at disagrees with the reported FirstContent"))?;
        }
        Ok(())
    }
}

sim_test! {
    /// A RawExchange's response is Complete exactly when its exchange's
    /// last stage was Completed, and Failed with that stage's failure
    /// otherwise.
    fn raw_response_variant_matches_final_stage(ctx) {
        let mut sim = sim(&ctx, Setup::default());
        let count = 4 + ctx.rng().below(non_zero(8)) as usize;
        let drawn = run_drawn(&ctx, &sim, count).await?;
        settle(&sim.stats, count as u64).await?;
        let stages = stages_by_exchange(drain_stages(&mut sim.stages));
        for exchange in drain(&mut sim) {
            let last = stages.get(&exchange.meta.id).and_then(|stages| stages.last()).cloned();
            let matches = match (&exchange.response, &last) {
                (RawResponse::Complete { .. }, Some(ExchangeStage::Completed)) => true,
                (RawResponse::Failed { failure, .. }, Some(ExchangeStage::Failed(stage))) => failure == stage,
                _ => false,
            };
            check(matches, || format!("{}: {:?} after final stage {last:?}", exchange.meta.model.0, failure_of(&exchange.response)))?;
            let scenario = drawn.get(&exchange.meta.model.0).copied();
            check(scenario.map(Scenario::failure) == Some(failure_of(&exchange.response)), || format!("{}: {scenario:?} ended {:?}", exchange.meta.model.0, failure_of(&exchange.response)))?;
        }
        Ok(())
    }
}

sim_test! {
    /// An upstream that closes before message_stop, cleanly or not, fails
    /// the exchange as truncated; it never enters Completed.
    fn upstream_close_before_finished_is_failure(ctx) {
        let case = stream_case();
        let mut sim = sim(&ctx, Setup::default());
        let frames = case.response.body.chunks().len();
        let mut rng = ctx.rng();
        let keep = rng.below(non_zero(frames as u64 - 1)) as usize;
        run(&sim, &case, Scenario::Truncated { keep }, "clean").await?;
        run(&sim, &case, Scenario::Disconnect { after: keep }, "cut").await?;
        settle(&sim.stats, 2).await?;
        let completed = drain_stages(&mut sim.stages).iter().any(|event| event.stage == ExchangeStage::Completed);
        check(!completed, || "an exchange cut short entered Completed".to_owned())?;
        for exchange in drain(&mut sim) {
            check(failure_of(&exchange.response) == Some(ExchangeFailure::StreamTruncated), || format!("{}: {:?}", exchange.meta.model.0, exchange.response))?;
        }
        Ok(())
    }
}

sim_test! {
    /// Every exchange's stages follow the proxy's machine: Forwarded, then
    /// optionally Responding, then exactly one of Completed or Failed.
    fn stage_transitions_follow_exchange_machine(ctx) {
        let mut sim = sim(&ctx, Setup::default());
        let count = 4 + ctx.rng().below(non_zero(8)) as usize;
        let _ = run_drawn(&ctx, &sim, count).await?;
        settle(&sim.stats, count as u64).await?;
        let stages = stages_by_exchange(drain_stages(&mut sim.stages));
        check(stages.len() == count, || format!("{} exchanges reported stages, expected {count}", stages.len()))?;
        for (id, stages) in stages {
            let legal = matches!(
                stages.as_slice(),
                [ExchangeStage::Forwarded { .. }, ExchangeStage::Responding { .. }, ExchangeStage::Completed | ExchangeStage::Failed(_)]
                    | [ExchangeStage::Forwarded { .. }, ExchangeStage::Failed(_)]
            );
            check(legal, || format!("{}: {stages:?}", id.ulid_text()))?;
        }
        Ok(())
    }
}

sim_test! {
    /// started_at <= first_chunk_at <= ended_at for every RawExchange, while
    /// the wall clock steps back and forward under the exchanges.
    fn stage_times_monotone_under_clock_steps(ctx) {
        let mut sim = sim(&ctx, Setup::default());
        let clock = ctx.clock();
        let mut rng = ctx.rng();
        let stepper = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(7)).await;
                let by = Duration::from_millis(1 + rng.below(non_zero(10_000)));
                clock.step(if rng.below(non_zero(2)) == 0 { ClockStep::Back(by) } else { ClockStep::Forward(by) });
            }
        });
        let case = stream_case();
        let count = 3 + ctx.rng().below(non_zero(5)) as usize;
        for n in 0..count {
            sim.upstream.push(SimReply::Scripted(Reply::from_case(&case).paced(Pacing::every(Duration::from_millis(5)))));
            let _ = open(&sim.proxy, &tagged(&case, &format!("model-{n}"))).await.ok_or_else(|| CheckFailed::new("no head"))?.collect().await;
        }
        settle(&sim.stats, count as u64).await?;
        stepper.abort();
        for exchange in drain(&mut sim) {
            let started = exchange.meta.started_at;
            let first = exchange.first_chunk_at.unwrap_or(started);
            check(started <= first && first <= exchange.ended_at, || format!("{}: {started:?} {first:?} {:?}", exchange.meta.model.0, exchange.ended_at))?;
        }
        Ok(())
    }
}

sim_test! {
    /// With decoding stalled, the upstream still receives the whole request
    /// and the client the whole response; capture waits for the decoder.
    fn forwarding_proceeds_while_decode_stalled(ctx) {
        let case = stream_case();
        let (mut sim, mut gate) = gated(&ctx, Setup::default());
        sim.upstream.push(SimReply::Scripted(Reply::from_case(&case)));
        let response = open(&sim.proxy, &case.request).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let (body, end) = response.collect().await;
        check(end == Read::End && body == *case.response_bytes(), || "the response was held for the decoder".to_owned())?;
        let received = sim.upstream.drain();
        check(received.len() == 1 && received[0].differences_from(&case.request).is_empty(), || "the request was held for the decoder".to_owned())?;
        tokio::time::sleep(SETTLE).await;
        check(gate.finished.try_recv().is_err() && sim.captured.try_recv().is_err(), || "captured without a decode".to_owned())?;
        gate.open();
        let exchange = capture_within(&mut sim.captured, SETTLE).await;
        check(exchange.is_some(), || "nothing captured after the decoder ran".to_owned())
    }
}

sim_test! {
    /// A paced response is relayed chunk by chunk to its end while its
    /// request's decode has not finished.
    fn response_relayed_before_decode_finishes(ctx) {
        let case = stream_case();
        let (mut sim, gate) = gated(&ctx, Setup::default());
        let pace = Duration::from_millis(1 + ctx.rng().below(non_zero(500)));
        sim.upstream.push(SimReply::Scripted(Reply::from_case(&case).paced(Pacing::every(pace))));
        let mut response = open(&sim.proxy, &case.request).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let started = Instant::now();
        let mut chunks = 0u32;
        let mut body = Vec::new();
        loop {
            match response.next().await {
                Read::Chunk { bytes, at } => {
                    chunks += 1;
                    body.extend_from_slice(&bytes);
                    check(at.duration_since(started) == pace * chunks, || format!("chunk {chunks} arrived at {:?}", at.duration_since(started)))?;
                }
                Read::End => break,
                Read::Aborted => return Err(CheckFailed::new("the response was cut")),
            }
        }
        check(body == case.response_bytes().to_vec(), || "the body changed".to_owned())?;
        check(sim.captured.try_recv().is_err(), || "captured before the decode".to_owned())?;
        gate.open();
        let exchange = capture_within(&mut sim.captured, SETTLE).await.ok_or_else(|| CheckFailed::new("nothing captured"))?;
        check(matches!(exchange.response, RawResponse::Complete { .. }), || format!("{:?}", exchange.response))
    }
}

sim_test! {
    /// A response that ends before its request has decoded is handed off
    /// only once the decode succeeds, with the decoded request and model.
    fn handoff_waits_for_decode_after_stream_end(ctx) {
        let case = stream_case();
        let (mut sim, gate) = gated(&ctx, Setup::default());
        sim.upstream.push(SimReply::Scripted(Reply::from_case(&case)));
        let _ = open(&sim.proxy, &case.request).await.ok_or_else(|| CheckFailed::new("no head"))?.collect().await;
        let wait = Duration::from_millis(1 + ctx.rng().below(non_zero(120_000)));
        tokio::time::sleep(wait).await;
        check(sim.captured.try_recv().is_err(), || "handed off before the decode".to_owned())?;
        gate.open();
        let exchange = capture_within(&mut sim.captured, SETTLE).await.ok_or_else(|| CheckFailed::new("nothing captured"))?;
        check(exchange.request.body == case.request.body.to_vec(), || "the request body is not the decoded body".to_owned())?;
        check(exchange.meta.model == exchange.request.harness.model && exchange.meta.model.0 == "claude-opus-5-5", || format!("model {:?}", exchange.meta.model))
    }
}

sim_test! {
    /// A request that decodes while its response is still streaming is
    /// handed off only when the stream ends.
    fn handoff_waits_for_stream_end_after_decode(ctx) {
        let case = stream_case();
        let (mut sim, mut gate) = gated(&ctx, Setup::default());
        gate.open();
        let (manual, feed) = Manual::event_stream();
        sim.upstream.push(SimReply::Manual(manual));
        let mut response = open(&sim.proxy, &case.request).await.ok_or_else(|| CheckFailed::new("no head"))?;
        let frames = case.response.body.chunks();
        let (last, rest) = frames.split_last().ok_or_else(|| CheckFailed::new("an empty case"))?;
        for frame in rest {
            let _ = feed.send(Feed::Data(frame.clone()));
            let _ = response.next().await;
        }
        let decoded = tokio::time::timeout(SETTLE, gate.finished.recv()).await;
        check(matches!(decoded, Ok(Some(()))), || "the request never decoded".to_owned())?;
        tokio::time::sleep(Duration::from_millis(1 + ctx.rng().below(non_zero(60_000)))).await;
        check(sim.captured.try_recv().is_err(), || "handed off before the stream ended".to_owned())?;
        let _ = feed.send(Feed::Data(last.clone()));
        drop(feed);
        let _ = response.collect().await;
        let exchange = capture_within(&mut sim.captured, SETTLE).await.ok_or_else(|| CheckFailed::new("nothing captured"))?;
        check(exchange.response == RawResponse::Complete { status: 200, body: case.response_bytes().to_vec() }, || format!("{:?}", failure_of(&exchange.response)))
    }
}
