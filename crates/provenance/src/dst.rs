//! Deterministic simulations of the bus consumer
//! (`crosstalk_provenance::dst::<name>`): the consumer task over the
//! in-process bus, `crosstalk-memory`'s reference index and the in-memory
//! records, on paused time with the simulation's clock, with seeded
//! delivery orders and redeliveries.

use std::sync::Arc;
use std::time::Duration;

use crosstalk_memory::provenance::{IndexConfig, MemoryFingerprintIndex};
use crosstalk_sim::{CheckFailed, SimClock, SimCtx};
use crosstalk_spec::derived::provenance::fingerprint::PositionedFingerprint;
use crosstalk_spec::derived::provenance::span::{Origin, OriginatedSpan, SpanState};
use crosstalk_spec::events::detect::DetectEvent;
use crosstalk_spec::events::ingest::{ConversationDelta, IngestEvent};
use crosstalk_spec::events::{BusEvent, Envelope, Subject};
use crosstalk_spec::ids::{AgentId, EventId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l2_transport::{
    ConsumerGroup, EventBus, RetryPolicy, Subscription,
};
use crosstalk_spec::interfaces::l4_provenance::{FingerprintIndex, SemanticMatcher};
use crosstalk_spec::observed::message::MessageBody;
use crosstalk_spec::support::{Clock, Timestamp};
use crosstalk_testkit::build::exchange::ExchangeBuilder;
use crosstalk_testkit::build::message::{assistant_text, message, tool_result};
use crosstalk_testkit::ids::Ids;
use crosstalk_transport::{BusConfig, MpscBus, MpscSubscription};
use tokio::task::JoinHandle;

use crate::config::{DecodeLimits, IndexSettings, ProvenanceConfig, winnow_params};
use crate::consumer::{self, ConsumerSettings, ConsumerStats};
use crate::engine::Provenance;
use crate::fingerprint::{Winnowing, positioned};
use crate::scan::messages::MemoryMessages;
use crate::store::{MemoryProvenanceStore, ProvenanceStore, ScanStatus};
use crate::tests::fixtures::{FakeSemantic, sentence};

const RETENTION: Duration = Duration::from_secs(60);
const EVICTION: Duration = Duration::from_secs(5);

fn dst_config() -> ProvenanceConfig {
    ProvenanceConfig::new(
        winnow_params(8, 4).expect("params"),
        DecodeLimits::default(),
        IndexSettings::single_node(50, RETENTION).expect("settings"),
        EVICTION,
        crosstalk_spec::support::Similarity::new(0.8).expect("threshold"),
    )
    .expect("config")
}

fn fail(message: impl Into<String>) -> CheckFailed {
    CheckFailed::new(message)
}

/// The consumer running over the bus, and handles on what it writes.
struct Rig {
    bus: MpscBus,
    observer: MpscSubscription,
    store: MemoryProvenanceStore,
    index: MemoryFingerprintIndex,
    semantic: FakeSemantic,
    messages: MemoryMessages,
    clock: SimClock,
    ids: Ids,
    published: Vec<Envelope>,
    task: JoinHandle<()>,
}

/// One exchange's two envelopes, and its delta.
struct Exchanged {
    captured: Envelope,
    delta_envelope: Envelope,
    delta: ConversationDelta,
}

impl Rig {
    async fn start(ctx: &SimCtx) -> Result<Rig, CheckFailed> {
        let config = dst_config();
        let bus = MpscBus::start(BusConfig::default()).map_err(|e| fail(format!("bus: {e:?}")))?;
        let retry = RetryPolicy::new(
            std::num::NonZeroU32::new(8).expect("attempts"),
            Duration::from_millis(10),
            Duration::from_millis(100),
        )
        .map_err(|e| fail(format!("retry: {e:?}")))?;
        let subscription = consumer::subscribe(&bus, retry)
            .await
            .map_err(|e| fail(format!("subscribe: {e:?}")))?;
        let observer = bus
            .subscribe(
                &[
                    Subject::SpanOriginated,
                    Subject::SpanRelayed,
                    Subject::ContentMatched,
                ],
                ConsumerGroup("observer".to_owned()),
                retry,
            )
            .await
            .map_err(|e| fail(format!("observer: {e:?}")))?;
        let index = MemoryFingerprintIndex::new(IndexConfig::single_node(
            config.index().cutoff(),
            config.index().retention(),
        ));
        let store = MemoryProvenanceStore::new();
        let messages = MemoryMessages::new();
        let semantic = FakeSemantic::default();
        let engine = Provenance::new(
            &config,
            index.clone(),
            store.clone(),
            semantic.clone(),
            messages.clone(),
        );
        let clock = ctx.clock();
        let settings = ConsumerSettings {
            retry_after: Duration::from_millis(10),
            eviction_interval: config.eviction_interval(),
        };
        let task = tokio::spawn(consumer::run(
            subscription,
            engine,
            bus.clone(),
            Arc::new(clock.clone()),
            settings,
            Arc::new(ConsumerStats::new()),
        ));
        Ok(Rig {
            bus,
            observer,
            store,
            index,
            semantic,
            messages,
            clock,
            ids: Ids::seeded(u32::try_from(ctx.seed().get() % u64::from(u32::MAX)).unwrap_or(0)),
            published: Vec::new(),
            task,
        })
    }

    fn envelope(&mut self, event: BusEvent, at: Timestamp) -> Envelope {
        Envelope {
            id: self.ids.id::<EventId>(),
            at,
            event,
        }
    }

    /// An exchange of `agent` now: `inputs` read as new inputs, `output`
    /// written; its bodies stored and its two envelopes built (not
    /// published).
    fn exchange(
        &mut self,
        agent: AgentId,
        inputs: Vec<MessageBody>,
        output: MessageBody,
    ) -> Exchanged {
        let now = self.clock.now();
        let inputs: Vec<_> = inputs.into_iter().map(message).collect();
        let output = message(output);
        for body in inputs.iter().chain(std::iter::once(&output)) {
            self.messages.put(body.clone());
        }
        let exchange = ExchangeBuilder::new(&mut self.ids)
            .started_at(now)
            .request(inputs.iter().map(|m| m.hash).collect())
            .response(output.hash)
            .build();
        let delta = ConversationDelta {
            exchange: exchange.meta.id,
            agent,
            conversation: self.ids.conversation(),
            new_inputs: inputs.iter().map(|m| m.hash).collect(),
            new_system: None,
            output: Some(output.hash),
        };
        let captured = self.envelope(
            BusEvent::Ingest(IngestEvent::ExchangeCaptured(Box::new(exchange))),
            now,
        );
        let delta_envelope = self.envelope(
            BusEvent::Ingest(IngestEvent::ConversationDelta(delta.clone())),
            now,
        );
        Exchanged {
            captured,
            delta_envelope,
            delta,
        }
    }

    async fn publish(&self, envelope: &Envelope) -> Result<(), CheckFailed> {
        self.bus
            .publish(envelope.clone())
            .await
            .map_err(|e| fail(format!("publish: {e:?}")))
    }

    /// Publish both envelopes and wait until the delta is processed.
    async fn deliver(&mut self, exchanged: &Exchanged) -> Result<(), CheckFailed> {
        self.publish(&exchanged.captured).await?;
        self.publish(&exchanged.delta_envelope).await?;
        self.processed(exchanged.delta.exchange).await
    }

    /// Wait until `exchange` is indexed (or failed).
    async fn processed(&self, exchange: ExchangeId) -> Result<(), CheckFailed> {
        for _ in 0..2000 {
            if let Ok(Some((_, status))) = self.store.exchange(exchange).await
                && matches!(
                    status,
                    ScanStatus::Indexed { .. } | ScanStatus::Failed { .. }
                )
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Err(fail("the delta was never processed"))
    }

    /// Collect what the consumer published so far.
    async fn drain(&mut self) -> Result<(), CheckFailed> {
        loop {
            match tokio::time::timeout(Duration::from_millis(20), self.observer.next()).await {
                Ok(Some(Ok(delivery))) => {
                    self.observer
                        .ack(delivery.id)
                        .await
                        .map_err(|e| fail(format!("ack: {e:?}")))?;
                    self.published.push(delivery.envelope);
                }
                Ok(Some(Err(error))) => return Err(fail(format!("observer: {error:?}"))),
                Ok(None) | Err(_) => return Ok(()),
            }
        }
    }

    /// Stop the consumer as a crash would (its task aborted mid-wait,
    /// nothing acked after) and start a new one, with a new engine, over the
    /// same records, index and bodies.
    async fn restart(&mut self) -> Result<(), CheckFailed> {
        self.task.abort();
        let _ = (&mut self.task).await;
        let config = dst_config();
        let retry = RetryPolicy::new(
            std::num::NonZeroU32::new(8).expect("attempts"),
            Duration::from_millis(10),
            Duration::from_millis(100),
        )
        .map_err(|e| fail(format!("retry: {e:?}")))?;
        let subscription = consumer::subscribe(&self.bus, retry)
            .await
            .map_err(|e| fail(format!("subscribe: {e:?}")))?;
        let engine = Provenance::new(
            &config,
            self.index.clone(),
            self.store.clone(),
            self.semantic.clone(),
            self.messages.clone(),
        );
        let settings = ConsumerSettings {
            retry_after: Duration::from_millis(10),
            eviction_interval: config.eviction_interval(),
        };
        self.task = tokio::spawn(consumer::run(
            subscription,
            engine,
            self.bus.clone(),
            Arc::new(self.clock.clone()),
            settings,
            Arc::new(ConsumerStats::new()),
        ));
        Ok(())
    }

    fn matches(&self) -> Vec<crate::store::StoredMatch> {
        self.store.all_matches()
    }

    async fn stop(self) {
        self.bus.shutdown().await;
        let _ = self.task.await;
    }
}

/// A one-dimensional embedding (the fake semantic store only keeps ids).
fn embedding() -> Result<crosstalk_spec::aggregates::topic::Embedding, CheckFailed> {
    let model = crosstalk_spec::aggregates::topic::EmbeddingModel {
        name: "fake".to_owned(),
        dimension: std::num::NonZeroU16::MIN,
    };
    crosstalk_spec::aggregates::topic::Embedding::new(model, vec![1.0])
        .map_err(|e| fail(format!("{e:?}")))
}

/// The fingerprints of `text`, positioned.
fn fingerprints(text: &str) -> Vec<PositionedFingerprint> {
    positioned(&Winnowing::new(dst_config().winnow()).winnow(text))
}

crosstalk_sim::sim_test! {
    /// `provenance.delta.redelivery-idempotent`: a delta delivered several
    /// times, interleaved with other traffic in a seeded order, leaves the
    /// spans, postings, frequencies and matches of one delivery, and every
    /// delivery publishes the same envelope ids.
    fn redelivered_delta_is_idempotent(ctx) {
        let mut rng = ctx.rng();
        let mut rig = Rig::start(&ctx).await?;
        let (a, b) = (rig.ids.agent(), rig.ids.agent());
        let text = sentence(&format!("idem-{}", ctx.seed().get()));
        let origin = rig.exchange(a, vec![], assistant_text(&text));
        rig.deliver(&origin).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let reader = rig.exchange(b, vec![tool_result("call_1", &text)], assistant_text(&sentence("b-out")));
        rig.deliver(&reader).await?;
        rig.drain().await?;
        let spans = rig.store.all_spans();
        let matches = rig.matches();
        let first: Vec<EventId> = rig.published.iter().map(|e| e.id).collect();
        let frequencies: Vec<u64> = {
            let mut out = Vec::new();
            for f in fingerprints(&text) {
                out.push(rig.index.frequency(f.fingerprint, rig.clock.now()).await.map_err(|e| fail(format!("{e:?}")))?);
            }
            out
        };
        let lookups = rig.index.lookup(&fingerprints(&text), rig.clock.now()).await.map_err(|e| fail(format!("{e:?}")))?;
        let redeliveries = 1 + rng.next_u64() % 3;
        for n in 0..redeliveries {
            if n % 2 == 1 {
                rig.publish(&origin.delta_envelope).await?;
            }
            rig.publish(&reader.delta_envelope).await?;
            tokio::time::sleep(Duration::from_millis(1 + rng.next_u64() % 20)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        rig.drain().await?;
        ctx.check(rig.store.all_spans() == spans, || "spans changed on redelivery".to_owned())?;
        ctx.check(rig.matches() == matches, || "matches changed on redelivery".to_owned())?;
        ctx.check(!matches.is_empty(), || "the read was matched".to_owned())?;
        let mut after = Vec::new();
        for f in fingerprints(&text) {
            after.push(rig.index.frequency(f.fingerprint, rig.clock.now()).await.map_err(|e| fail(format!("{e:?}")))?);
        }
        ctx.check(after == frequencies, || format!("frequencies {frequencies:?} became {after:?}"))?;
        let lookups_after = rig.index.lookup(&fingerprints(&text), rig.clock.now()).await.map_err(|e| fail(format!("{e:?}")))?;
        ctx.check(lookups_after.len() == lookups.len(), || "postings changed on redelivery".to_owned())?;
        let republished: std::collections::BTreeSet<EventId> = rig.published.iter().map(|e| e.id).collect();
        let original: std::collections::BTreeSet<EventId> = first.iter().copied().collect();
        ctx.check(republished == original, || "a redelivery published a new envelope id".to_owned())?;
        rig.stop().await;
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// `transport.consumer.derived-envelope-ids`: every envelope the
    /// consumer publishes for a delta has the id derived from the delta's
    /// exchange and the record it announces (`span::span_event_id`,
    /// `span::match_id`), so a redelivery, a republish of the delta under a
    /// new envelope id, and a redelivery to a consumer restarted over the
    /// same records all land on the ids of the first delivery.
    fn redelivery_republishes_the_same_envelope_ids(ctx) {
        let mut rng = ctx.rng();
        let mut rig = Rig::start(&ctx).await?;
        let (a, b) = (rig.ids.agent(), rig.ids.agent());
        let text = sentence(&format!("derived-{}", ctx.seed().get()));
        let origin = rig.exchange(a, vec![], assistant_text(&text));
        rig.deliver(&origin).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        let reader = rig.exchange(b, vec![tool_result("call_1", &text)], assistant_text(&sentence("b-said")));
        rig.deliver(&reader).await?;
        rig.drain().await?;
        let first: Vec<EventId> = rig.published.iter().map(|e| e.id).collect();
        ctx.check(!rig.matches().is_empty(), || "the read was matched".to_owned())?;

        // Each id is the one derived from what the envelope announces.
        let mut derived = std::collections::BTreeSet::new();
        for record in rig.store.all_spans() {
            if matches!(record.span.state.origin(), Some(Origin::Originated | Origin::Relayed(_))) {
                derived.insert(crate::span::span_event_id(record.span.exchange, record.span.id));
            }
        }
        for stored in rig.matches() {
            let content = &stored.content;
            derived.insert(crate::span::match_id(content.reader_exchange(), content.origin(), &content.read_at()));
        }
        let published: std::collections::BTreeSet<EventId> = first.iter().copied().collect();
        ctx.check(published.len() == first.len(), || "an id was published twice by one delivery".to_owned())?;
        ctx.check(published == derived, || format!("published {published:?}, derived {derived:?}"))?;

        // Redeliveries: the same envelope, a republish under a new id, and,
        // in a seeded order, after a restart of the consumer.
        let restart_at = rng.next_u64() % 3;
        for n in 0..3u64 {
            if n == restart_at {
                rig.restart().await?;
            }
            let delivery = match n {
                0 => reader.delta_envelope.clone(),
                1 => rig.envelope(reader.delta_envelope.event.clone(), rig.clock.now()),
                _ => rig.envelope(origin.delta_envelope.event.clone(), rig.clock.now()),
            };
            rig.publish(&delivery).await?;
            tokio::time::sleep(Duration::from_millis(1 + rng.next_u64() % 20)).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        rig.drain().await?;
        let again: std::collections::BTreeSet<EventId> = rig.published.iter().map(|e| e.id).collect();
        ctx.check(again == published, || format!("redeliveries published {again:?}, first {published:?}"))?;
        ctx.check(rig.published.len() > first.len(), || "nothing was republished".to_owned())?;
        rig.stop().await;
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// `provenance.match.indexed-before-read`: whichever of the origin's
    /// and the reader's deltas the seed delivers first, a match exists only
    /// when the origin span was indexed before the read was processed.
    fn match_only_against_spans_indexed_before_read(ctx) {
        let mut rng = ctx.rng();
        let mut rig = Rig::start(&ctx).await?;
        let (a, b) = (rig.ids.agent(), rig.ids.agent());
        let text = sentence(&format!("order-{}", ctx.seed().get()));
        let origin = rig.exchange(a, vec![], assistant_text(&text));
        let reader = rig.exchange(b, vec![tool_result("call_1", &text)], assistant_text(&sentence("reply")));
        let origin_first = rng.next_u64().is_multiple_of(2);
        if origin_first {
            rig.deliver(&origin).await?;
            rig.deliver(&reader).await?;
        } else {
            rig.deliver(&reader).await?;
            rig.deliver(&origin).await?;
        }
        let matches = rig.matches();
        ctx.check(matches.is_empty() != origin_first, || format!("origin first: {origin_first}, matches: {}", matches.len()))?;
        for stored in &matches {
            let origin_span = rig.store.span(stored.content.origin()).await.map_err(|e| fail(format!("{e:?}")))?;
            let indexed = origin_span.and_then(|r| r.index_seq).is_some();
            ctx.check(indexed, || "a match names a span that was never indexed".to_owned())?;
        }
        rig.stop().await;
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// `provenance.index.retention-bound` and
    /// `provenance.semantic.retention-bound`: once retention plus one
    /// eviction interval has passed, the index holds no posting and no
    /// observation from the scanned texts, the semantic store holds no
    /// expired span, and every originated span is expired.
    fn derived_data_evicted_after_retention(ctx) {
        let mut rng = ctx.rng();
        let mut rig = Rig::start(&ctx).await?;
        let agents: Vec<AgentId> = (0..3).map(|_| rig.ids.agent()).collect();
        let mut texts = Vec::new();
        for n in 0..(2 + rng.next_u64() % 4) {
            let agent = agents[(rng.next_u64() % 3) as usize];
            let text = sentence(&format!("ret-{}-{n}", ctx.seed().get()));
            let read = texts.last().cloned().unwrap_or_else(|| sentence("first read"));
            let exchanged = rig.exchange(agent, vec![tool_result("call", &read)], assistant_text(&text));
            rig.deliver(&exchanged).await?;
            texts.push(text);
            tokio::time::sleep(Duration::from_secs(rng.next_u64() % 20)).await;
        }
        let mut semantic = rig.semantic.clone();
        for record in rig.store.all_spans() {
            if let Some(span) = OriginatedSpan::new(record.span.clone()) {
                semantic.insert(&span, embedding()?).await.map_err(|e| fail(format!("{e:?}")))?;
            }
        }
        tokio::time::sleep(RETENTION + EVICTION + Duration::from_secs(1)).await;
        let now = rig.clock.now();
        for text in &texts {
            for f in fingerprints(text) {
                let frequency = rig.index.frequency(f.fingerprint, Timestamp::from_micros(0)).await.map_err(|e| fail(format!("{e:?}")))?;
                ctx.check(frequency == 0, || "an observation outlived retention".to_owned())?;
            }
            let hits = rig.index.lookup(&fingerprints(text), now).await.map_err(|e| fail(format!("{e:?}")))?;
            ctx.check(hits.is_empty(), || "a posting outlived retention".to_owned())?;
        }
        ctx.check(rig.semantic.stored().is_empty(), || "the semantic store kept an expired span".to_owned())?;
        for record in rig.store.all_spans() {
            let live = matches!(record.span.state, SpanState::Indexed { .. } | SpanState::Propagated { .. });
            ctx.check(!live, || "a span outlived retention".to_owned())?;
        }
        rig.stop().await;
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// `provenance.match.none-after-expiry`: a read processed after the
    /// origin span expired gets no match, whatever the time between.
    fn expired_span_never_matched(ctx) {
        let mut rng = ctx.rng();
        let mut rig = Rig::start(&ctx).await?;
        let (a, b) = (rig.ids.agent(), rig.ids.agent());
        let text = sentence(&format!("expire-{}", ctx.seed().get()));
        let origin = rig.exchange(a, vec![], assistant_text(&text));
        rig.deliver(&origin).await?;
        let wait = RETENTION + EVICTION + Duration::from_secs(1 + rng.next_u64() % 30);
        tokio::time::sleep(wait).await;
        let span: Vec<SpanId> = rig.store.all_spans().iter().map(|r| r.span.id).collect();
        let reader = rig.exchange(b, vec![tool_result("call_1", &text)], assistant_text(&sentence("late")));
        rig.deliver(&reader).await?;
        let late: Vec<_> = rig.matches().into_iter().filter(|m| span.contains(&m.content.origin())).collect();
        ctx.check(late.is_empty(), || "an expired span was matched".to_owned())?;
        rig.stop().await;
        Ok(())
    }
}

crosstalk_sim::sim_test! {
    /// The consumer publishes `ContentMatched` for a read, `SpanOriginated`
    /// for the origin's text, and acks an `ExchangeCaptured` that arrives
    /// after its delta (the delta is retried until it can be scanned).
    fn consumer_publishes_content_matched(ctx) {
        let mut rig = Rig::start(&ctx).await?;
        let (a, b) = (rig.ids.agent(), rig.ids.agent());
        let text = sentence(&format!("pub-{}", ctx.seed().get()));
        let origin = rig.exchange(a, vec![], assistant_text(&text));
        rig.deliver(&origin).await?;
        let reader = rig.exchange(b, vec![tool_result("call_9", &text)], assistant_text(&sentence("ok")));
        rig.publish(&reader.delta_envelope).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        rig.publish(&reader.captured).await?;
        rig.processed(reader.delta.exchange).await?;
        rig.drain().await?;
        let originated = rig.published.iter().any(|e| matches!(e.event, BusEvent::Detect(DetectEvent::SpanOriginated { agent, .. }) if agent == a));
        let matched = rig.published.iter().any(|e| matches!(&e.event, BusEvent::Detect(DetectEvent::ContentMatched(m)) if m.reader() == b && m.origin_agent() == a));
        ctx.check(originated, || "no SpanOriginated".to_owned())?;
        ctx.check(matched, || "no ContentMatched".to_owned())?;
        rig.stop().await;
        Ok(())
    }
}
