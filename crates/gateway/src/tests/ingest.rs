//! `Pipeline::ingest` under simulation: the same envelope and blobs as the
//! proxy path for the same exchange, retries under blob store faults and a
//! typed failure once they run out, and monotonic envelope ids under many
//! concurrent ingests.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_canonical::{AnthropicMessages, StoreError};
use crosstalk_sim::{CheckFailed, DurationRange, Probability, SimCtx, StoreFaults, Timed};
use crosstalk_spec::events::Envelope;
use crosstalk_spec::ids::{EventId, MessageHash, SeededRandom};
use crosstalk_spec::interfaces::l1_canonical::{NormalizedExchange, Normalizer};
use crosstalk_spec::interfaces::l2_transport::BlobStore;
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_testkit::ids::Ids;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus};
use tokio::sync::mpsc;

use super::raw;
use super::record::{RecordingBus, RecordingStore, drain};
use crate::pipeline::{Deps, IngestError, Pipeline, PipelineCounts, PutRetry, Settings};

fn failed(what: &str, error: impl std::fmt::Debug) -> CheckFailed {
    CheckFailed::new(format!("{what}: {error:?}"))
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn ids_for(ctx: &SimCtx) -> Ids {
    Ids::seeded(u32::try_from(ctx.seed().get() % u64::from(u32::MAX)).unwrap_or(0))
}

fn bus() -> Result<MpscBus, CheckFailed> {
    MpscBus::start(BusConfig::default()).map_err(|error| failed("bus", error))
}

/// Every exchange of the corpus (and the image request), normalized.
fn normalized(ids: &mut Ids) -> Result<Vec<NormalizedExchange>, CheckFailed> {
    raw::exchanges(ids)
        .iter()
        .map(|raw| {
            AnthropicMessages
                .normalize(raw)
                .map_err(|error| failed("normalize", error))
        })
        .collect()
}

/// Every blob an exchange's ingest stores: message bodies, then media.
fn blob_hashes(exchange: &NormalizedExchange) -> Vec<MessageHash> {
    exchange
        .messages
        .iter()
        .map(|message| message.hash)
        .chain(exchange.media.iter().map(|media| media.hash()))
        .collect()
}

/// For each exchange of the corpus: a pipeline fed through the proxy's
/// capture channel (normalized by the capture stage) and one fed the same
/// exchange through `ingest`, normalized beforehand, at the same simulated
/// instant, put the same bytes in the same order and publish the same
/// envelope (id, time and event).
pub async fn ingest_matches_the_proxy_path(ctx: SimCtx) -> Result<(), CheckFailed> {
    let seed = ctx.seed().get();
    let clock: Arc<dyn Clock> = Arc::new(ctx.clock());

    let proxy_blobs = MemoryBlobStore::new();
    let (store, mut proxy_puts) = RecordingStore::new(proxy_blobs.clone());
    let (recording, mut proxy_sent) = RecordingBus::new(bus()?);
    let (sender, captured) = mpsc::channel(1);
    let mut proxy = Pipeline::build(
        Settings::default(),
        Deps {
            capture: Some(captured),
            ..Deps::stores(store, recording, SeededRandom::new(seed))
        },
        Arc::clone(&clock),
    )
    .await
    .map_err(|error| failed("proxy pipeline", error))?;

    let ingest_blobs = MemoryBlobStore::new();
    let (store, mut ingest_puts) = RecordingStore::new(ingest_blobs.clone());
    let (recording, mut ingest_sent) = RecordingBus::new(bus()?);
    let ingest = Pipeline::build(
        Settings::default(),
        Deps::stores(store, recording, SeededRandom::new(seed)),
        Arc::clone(&clock),
    )
    .await
    .map_err(|error| failed("ingest pipeline", error))?;

    let mut ids = ids_for(&ctx);
    let raws = raw::exchanges(&mut ids);
    let sent = raws.len();
    let mut rng = ctx.rng();
    let gap = DurationRange::new(ms(1), ms(500)).map_err(|error| failed("range", error))?;
    ctx.step("feed");
    for raw in raws {
        tokio::time::sleep(rng.duration_in(gap)).await;
        let case = raw.meta.id.ulid_text();
        let exchange = AnthropicMessages
            .normalize(&raw)
            .map_err(|error| failed("normalize", error))?;
        let at = clock.now();
        let id = ingest
            .ingest(exchange, at)
            .await
            .map_err(|error| failed("ingest", error))?;
        sender
            .send(raw)
            .await
            .map_err(|_| CheckFailed::new("the capture stage stopped early"))?;
        let captured = proxy_sent
            .recv()
            .await
            .ok_or_else(|| CheckFailed::new("the proxy path published nothing"))?;
        let ingested: Vec<Envelope> = drain(&mut ingest_sent);
        ctx.check(ingested == [captured.clone()], || {
            format!("{case}: ingest published {ingested:?}, the proxy path {captured:?}")
        })?;
        ctx.check(id == captured.id && captured.at == at, || {
            format!(
                "{case}: ingest returned {id:?} at {at:?}; the proxy path published {:?} at {:?}",
                captured.id, captured.at
            )
        })?;
        let (by_proxy, by_ingest) = (drain(&mut proxy_puts), drain(&mut ingest_puts));
        ctx.check(!by_proxy.is_empty() && by_proxy == by_ingest, || {
            format!(
                "{case}: the proxy path put {} blobs, ingest {}, or they differ",
                by_proxy.len(),
                by_ingest.len()
            )
        })?;
    }
    drop(sender);
    ctx.step("drain");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    ctx.check(proxy.join_capture(deadline).await, || {
        "the capture stage did not drain".to_owned()
    })?;
    let stored = (proxy_blobs.len(), ingest_blobs.len());
    ctx.check(matches!(stored, (Ok(a), Ok(b)) if a == b && a > 0), || {
        format!("blob counts differ: {stored:?}")
    })?;
    let published = u64::try_from(sent).unwrap_or(u64::MAX);
    let expected = PipelineCounts {
        published,
        ..PipelineCounts::default()
    };
    let counts = (proxy.stats().snapshot(), ingest.stats().snapshot());
    ctx.check(counts == (expected, expected), || {
        format!("counts {counts:?}, expected {published} published each")
    })?;
    ctx.note(format!("{sent} exchanges, {} blobs", stored.0.unwrap_or(0)));
    Ok(())
}

/// A put that always fails, before or after it commits: `ingest` makes
/// exactly `attempts` attempts `backoff` apart (each stops at its first
/// failed put), publishes nothing, and returns `NotStored`. A put that
/// committed and reported failure is rewritten idempotently. Then, under
/// transient failures, every exchange is stored and published, with one
/// retry per failed put.
pub async fn ingest_retries_blob_faults_then_fails_typed(ctx: SimCtx) -> Result<(), CheckFailed> {
    let seed = ctx.seed().get();
    let clock: Arc<dyn Clock> = Arc::new(ctx.clock());
    let exchanges = normalized(&mut ids_for(&ctx))?;
    let retry = PutRetry {
        attempts: NonZeroU32::MIN.saturating_add(3),
        backoff: ms(25),
    };
    let settings = Settings {
        put_retry: retry,
        ..Settings::default()
    };

    for (node, faults, committed) in [
        (
            "fail-before",
            StoreFaults {
                fail_before: Probability::ALWAYS,
                ..StoreFaults::none()
            },
            0,
        ),
        (
            "fail-after",
            StoreFaults {
                fail_after: Probability::ALWAYS,
                ..StoreFaults::none()
            },
            1,
        ),
    ] {
        ctx.step(node);
        let inner = MemoryBlobStore::new();
        let faulty = ctx.faulty_store(inner.clone(), faults, &ctx.node(node).handle());
        let (store, mut puts) = RecordingStore::new(faulty);
        let (recording, mut sent) = RecordingBus::new(bus()?);
        let pipeline = Pipeline::build(
            settings,
            Deps::stores(store, recording, SeededRandom::new(seed)),
            Arc::clone(&clock),
        )
        .await
        .map_err(|error| failed("pipeline", error))?;
        let exchange = exchanges
            .first()
            .cloned()
            .ok_or_else(|| CheckFailed::new("no exchange"))?;
        let first = blob_hashes(&exchange).first().copied();
        let started = tokio::time::Instant::now();
        let result = pipeline.ingest(exchange, clock.now()).await;
        let waited = started.elapsed();
        ctx.check(
            matches!(
                &result,
                Err(IngestError::NotStored { attempts, source: StoreError::Blob { .. } })
                    if *attempts == retry.attempts
            ),
            || format!("{node}: {result:?}"),
        )?;
        ctx.check(waited == retry.backoff * 3, || {
            format!("{node}: three backoffs should pass, {waited:?} did")
        })?;
        let puts = drain(&mut puts);
        ctx.check(
            puts.len() == 4
                && puts.iter().all(|put| !put.ok)
                && puts.windows(2).all(|pair| pair[0].bytes == pair[1].bytes),
            || format!("{node}: four failed puts of the first blob, got {puts:?}"),
        )?;
        ctx.check(drain(&mut sent).is_empty(), || {
            format!("{node}: published after the store failed")
        })?;
        let counts = pipeline.stats().snapshot();
        ctx.check(
            counts
                == PipelineCounts {
                    store_failed: 1,
                    store_retries: 3,
                    ..PipelineCounts::default()
                },
            || format!("{node}: {counts:?}"),
        )?;
        ctx.check(inner.len() == Ok(committed), || {
            format!(
                "{node}: {:?} blobs committed, expected {committed}",
                inner.len()
            )
        })?;
        if committed == 1 {
            let stored = first.map(|hash| inner.get(hash));
            let stored = match stored {
                Some(get) => get.await.map(|bytes| bytes.is_some()),
                None => Ok(false),
            };
            ctx.check(stored == Ok(true), || {
                format!("{node}: the committed blob is not the first one")
            })?;
        }
    }

    ctx.step("transient");
    let inner = MemoryBlobStore::new();
    let faults = StoreFaults {
        latency: Some(Timed::new(
            Probability::percent(30).map_err(|error| failed("probability", error))?,
            DurationRange::new(ms(1), ms(20)).map_err(|error| failed("range", error))?,
        )),
        fail_before: Probability::percent(10).map_err(|error| failed("probability", error))?,
        ..StoreFaults::none()
    };
    let faulty = ctx.faulty_store(inner.clone(), faults, &ctx.node("transient").handle());
    let (store, mut puts) = RecordingStore::new(faulty);
    let (recording, mut sent) = RecordingBus::new(bus()?);
    let pipeline = Pipeline::build(
        Settings {
            put_retry: PutRetry {
                attempts: NonZeroU32::MIN.saturating_add(63),
                backoff: ms(5),
            },
            ..Settings::default()
        },
        Deps::stores(store, recording, SeededRandom::new(seed)),
        Arc::clone(&clock),
    )
    .await
    .map_err(|error| failed("pipeline", error))?;
    let total = exchanges.len();
    for exchange in exchanges {
        let hashes = blob_hashes(&exchange);
        let case = exchange.exchange.meta.id.ulid_text();
        pipeline
            .ingest(exchange, clock.now())
            .await
            .map_err(|error| failed(&format!("{case} under transient faults"), error))?;
        for hash in hashes {
            ctx.check(matches!(inner.get(hash).await, Ok(Some(_))), || {
                format!("{case}: blob {hash:?} missing after ingest returned Ok")
            })?;
        }
    }
    let failed_puts = drain(&mut puts).iter().filter(|put| !put.ok).count();
    let counts = pipeline.stats().snapshot();
    ctx.check(
        drain(&mut sent).len() == total
            && counts.published == u64::try_from(total).unwrap_or(0)
            && counts.store_retries == u64::try_from(failed_puts).unwrap_or(u64::MAX)
            && counts.store_failed == 0,
        || format!("{total} exchanges, {failed_puts} failed puts: {counts:?}"),
    )?;
    ctx.note(format!("{failed_puts} transient put failures retried"));
    Ok(())
}

/// Many ingests at once, over a slow store and a slow bus, with times that
/// go back and forth: every one is published, ids are distinct, reach the bus in
/// strictly increasing order, and never carry a millisecond before `at`.
pub async fn concurrent_ingests_keep_ids_monotonic(ctx: SimCtx) -> Result<(), CheckFailed> {
    let seed = ctx.seed().get();
    let clock: Arc<dyn Clock> = Arc::new(ctx.clock());
    let faults = StoreFaults {
        latency: Some(Timed::new(
            Probability::percent(60).map_err(|error| failed("probability", error))?,
            DurationRange::new(ms(1), ms(40)).map_err(|error| failed("range", error))?,
        )),
        ..StoreFaults::none()
    };
    let blobs = ctx.faulty_store(MemoryBlobStore::new(), faults, &ctx.node("store").handle());
    // A slow bus: acceptance order follows mint order only if ingest
    // serializes minting and publishing.
    let (recording, mut sent) = RecordingBus::slow(bus()?, 13);
    let pipeline = Pipeline::build(
        Settings::default(),
        Deps::stores(blobs, recording, SeededRandom::new(seed)),
        Arc::clone(&clock),
    )
    .await
    .map_err(|error| failed("pipeline", error))?;

    let mut ids = ids_for(&ctx);
    let mut exchanges = Vec::new();
    for _ in 0..4 {
        exchanges.extend(normalized(&mut ids)?);
    }
    let base = clock.now().as_micros();
    let mut rng = ctx.rng();
    let spread =
        DurationRange::new(Duration::ZERO, ms(20)).map_err(|error| failed("range", error))?;
    ctx.step("ingest");
    let mut tasks = Vec::new();
    for (index, exchange) in exchanges.into_iter().enumerate() {
        let offset = u64::try_from(rng.duration_in(spread).as_micros()).unwrap_or(0);
        let at = Timestamp::from_micros(base + offset);
        let ingester = pipeline.ingester();
        tasks.push(ctx.spawn(&format!("ingest-{index}"), async move {
            (at, ingester.ingest(exchange, at).await)
        }));
    }
    let mut returned: BTreeMap<EventId, Timestamp> = BTreeMap::new();
    for task in tasks {
        let (at, result) = task
            .join()
            .await
            .map_err(|error| failed("ingest task", error))?;
        let id = result.map_err(|error| failed("ingest", error))?;
        ctx.check(returned.insert(id, at).is_none(), || {
            format!("{id:?} was returned twice")
        })?;
    }
    let published = drain(&mut sent);
    ctx.check(published.len() == returned.len(), || {
        format!("{} returned, {} published", returned.len(), published.len())
    })?;
    ctx.check(
        published.windows(2).all(|pair| pair[0].id < pair[1].id),
        || "envelope ids reached the bus out of order".to_owned(),
    )?;
    let ids: BTreeSet<EventId> = published.iter().map(|envelope| envelope.id).collect();
    ctx.check(ids.iter().eq(returned.keys()), || {
        "the published ids are not the returned ones".to_owned()
    })?;
    for envelope in &published {
        let at = returned.get(&envelope.id).copied();
        let millis = u64::try_from(envelope.id.as_ulid() >> 80).unwrap_or(u64::MAX);
        ctx.check(
            at == Some(envelope.at) && millis >= envelope.at.as_micros() / 1000,
            || {
                format!(
                    "{:?} at {:?} carries millisecond {millis}",
                    envelope.id, envelope.at
                )
            },
        )?;
    }
    ctx.note(format!("{} concurrent ingests", published.len()));
    Ok(())
}
