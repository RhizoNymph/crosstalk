//! `canonical.capture.blobs-before-event` under simulation: the capture
//! stage over a blob store that is slow and fails before or after puts,
//! fed at seeded random times, observed by a consumer that checks, on each
//! `ExchangeCaptured` it receives, that every blob the exchange references
//! (message bodies and media) is already in the store.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crosstalk_canonical::AnthropicMessages;
use crosstalk_sim::{CheckFailed, DurationRange, Probability, SimCtx, StoreFaults, Timed};
use crosstalk_spec::events::ingest::IngestEvent;
use crosstalk_spec::events::{BusEvent, Subject};
use crosstalk_spec::ids::{ExchangeId, MessageHash};
use crosstalk_spec::interfaces::l1_canonical::Normalizer;
use crosstalk_spec::interfaces::l2_transport::{BlobStore, ConsumerGroup, EventBus, Subscription};
use crosstalk_spec::observed::exchange::{Exchange, ExchangeOutcome};
use crosstalk_testkit::ids::Ids;
use crosstalk_transport::blob::MemoryBlobStore;
use crosstalk_transport::{BusConfig, MpscBus};
use tokio::sync::mpsc;

use super::raw;
use crate::capture::{CaptureStage, EventIds, PipelineStats, PutRetry};

fn failed(what: &str, error: impl std::fmt::Debug) -> CheckFailed {
    CheckFailed::new(format!("{what}: {error:?}"))
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn referenced(exchange: &Exchange) -> Vec<MessageHash> {
    let mut hashes = exchange.request.clone();
    match &exchange.outcome {
        ExchangeOutcome::Completed { response, .. } => hashes.push(*response),
        ExchangeOutcome::Failed {
            partial_response, ..
        } => hashes.extend(partial_response.iter().copied()),
    }
    hashes
}

/// What one observed delivery found.
struct Observed {
    exchange: ExchangeId,
    missing: Vec<MessageHash>,
}

pub async fn blobs_written_before_capture_published(ctx: SimCtx) -> Result<(), CheckFailed> {
    let seed = ctx.seed().get();
    let inner = MemoryBlobStore::new();
    let node = ctx.node("capture");
    let faults = StoreFaults {
        latency: Some(Timed::new(
            Probability::percent(40).map_err(|error| failed("probability", error))?,
            DurationRange::new(ms(1), ms(30)).map_err(|error| failed("range", error))?,
        )),
        fail_before: Probability::percent(10).map_err(|error| failed("probability", error))?,
        fail_after: Probability::percent(10).map_err(|error| failed("probability", error))?,
        crash_after: Probability::NEVER,
    };
    let blobs = ctx.faulty_store(inner.clone(), faults, &node.handle());
    let config = BusConfig::default();
    let bus = MpscBus::start(config.clone()).map_err(|error| failed("bus", error))?;
    let mut observer = bus
        .subscribe(
            &[Subject::ExchangeCaptured],
            ConsumerGroup("observer".to_owned()),
            config.retry,
        )
        .await
        .map_err(|error| failed("subscribe", error))?;

    let mut ids = Ids::seeded(u32::try_from(seed % u64::from(u32::MAX)).unwrap_or(0));
    let raws = raw::exchanges(&mut ids);
    // Everything each exchange references, media included, known up front.
    let mut expected: BTreeMap<ExchangeId, Vec<MessageHash>> = BTreeMap::new();
    for raw in &raws {
        let normalization = AnthropicMessages
            .normalize(raw)
            .map_err(|error| failed("normalize", error))?;
        let mut hashes: Vec<MessageHash> = normalization
            .messages
            .iter()
            .map(|message| message.hash)
            .collect();
        hashes.extend(normalization.media.iter().map(|media| media.hash()));
        expected.insert(raw.meta.id, hashes);
    }
    let sent = raws.len();

    let stats = Arc::new(PipelineStats::new());
    let retry = PutRetry {
        attempts: NonZeroU32::MIN.saturating_add(1),
        backoff: ms(5),
    };
    let stage = CaptureStage::new(
        blobs,
        bus.clone(),
        Arc::new(ctx.clock()),
        EventIds::seeded(seed),
        Arc::clone(&stats),
        retry,
    );
    let (sender, captured) = mpsc::channel(4);
    let stage = ctx.spawn("capture", stage.run(captured));

    let store = inner.clone();
    let lookup = expected.clone();
    let watcher = ctx.spawn("observer", async move {
        let mut seen = Vec::new();
        while let Some(Ok(delivery)) = observer.next().await {
            let BusEvent::Ingest(IngestEvent::ExchangeCaptured(exchange)) =
                &delivery.envelope.event
            else {
                continue;
            };
            let mut wanted = lookup.get(&exchange.meta.id).cloned().unwrap_or_default();
            wanted.extend(referenced(exchange));
            let mut missing = Vec::new();
            for hash in wanted {
                if !matches!(store.get(hash).await, Ok(Some(_))) {
                    missing.push(hash);
                }
            }
            seen.push(Observed {
                exchange: exchange.meta.id,
                missing,
            });
            let _ = observer.ack(delivery.id).await;
        }
        seen
    });

    let mut rng = ctx.rng();
    let gap = DurationRange::new(Duration::ZERO, ms(20)).map_err(|error| failed("range", error))?;
    ctx.step("feed");
    for raw in raws {
        tokio::time::sleep(rng.duration_in(gap)).await;
        sender
            .send(raw)
            .await
            .map_err(|_| CheckFailed::new("the capture stage stopped early"))?;
    }
    drop(sender);
    stage
        .join()
        .await
        .map_err(|error| failed("capture stage", error))?;
    ctx.step("drain");
    let observer_group = ConsumerGroup("observer".to_owned());
    loop {
        match bus.depth(&observer_group).await {
            Ok(Some(depth)) if depth.ready + depth.delayed + depth.held + depth.waiting > 0 => {
                tokio::time::sleep(ms(1)).await;
            }
            _ => break,
        }
    }
    bus.shutdown().await;
    let seen = watcher
        .join()
        .await
        .map_err(|error| failed("observer", error))?;

    let counts = stats.snapshot();
    for observed in &seen {
        ctx.check(observed.missing.is_empty(), || {
            format!(
                "ExchangeCaptured for {} arrived before blobs {:?} were stored",
                observed.exchange.ulid_text(),
                observed.missing
            )
        })?;
    }
    let distinct: BTreeSet<ExchangeId> = seen.iter().map(|observed| observed.exchange).collect();
    ctx.check(distinct.len() == seen.len(), || {
        "an exchange was published twice".to_owned()
    })?;
    ctx.check(
        u64::try_from(seen.len()).unwrap_or(u64::MAX) == counts.published,
        || {
            format!(
                "observed {} events, published {}",
                seen.len(),
                counts.published
            )
        },
    )?;
    ctx.check(
        counts.published + counts.store_failed == u64::try_from(sent).unwrap_or(0)
            && counts.normalize_failed == 0
            && counts.publish_failed == 0,
        || format!("every exchange is published or given up: {counts:?}"),
    )?;
    ctx.note(format!(
        "published {} of {sent}, {} given up, {} retries",
        counts.published, counts.store_failed, counts.store_retries
    ));
    Ok(())
}
