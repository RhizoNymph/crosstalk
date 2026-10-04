//! Test doubles for the ports the in-memory reference stores do not cover:
//! a bus that records what the surface publishes (and can refuse), and the
//! evidence records (spans, accesses, resources by id).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use crosstalk_spec::derived::flow::access::Access;
use crosstalk_spec::derived::flow::resource::Resource;
use crosstalk_spec::derived::provenance::span::Span;
use crosstalk_spec::events::{Envelope, Subject};
use crosstalk_spec::ids::{AccessId, ResourceId, SpanId};
use crosstalk_spec::interfaces::l2_transport::{
    BusError, ConsumerGroup, Delivery, DeliveryId, EventBus, RetryPolicy, Subscription,
};

use crosstalk_memory::analysis::fakes::FakeEmbedder;
use crosstalk_spec::aggregates::topic::{Embedding, EmbeddingModel};
use crosstalk_spec::interfaces::l6_analysis::{EmbedError, Embedder};

use crate::stores::{EvidenceRecords, RecordReadError};

/// [`FakeEmbedder`] that counts the texts it embeds and whose model a test
/// can switch.
#[derive(Debug, Clone)]
pub struct CountingEmbedder {
    inner: Arc<Mutex<FakeEmbedder>>,
    embedded: Arc<Mutex<usize>>,
}

impl CountingEmbedder {
    pub fn new(inner: FakeEmbedder) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
            embedded: Arc::new(Mutex::new(0)),
        }
    }

    /// How many texts were embedded so far.
    pub fn embedded(&self) -> usize {
        *lock(&self.embedded)
    }

    /// Embed with `model` from now on.
    pub fn switch(&self, model: EmbeddingModel) {
        *lock(&self.inner) = FakeEmbedder::new(model, 400);
    }
}

impl Embedder for CountingEmbedder {
    fn model(&self) -> EmbeddingModel {
        lock(&self.inner).model()
    }

    async fn embed(&self, texts: &[&str]) -> Result<Vec<Embedding>, EmbedError> {
        *lock(&self.embedded) += texts.len();
        let inner = lock(&self.inner).clone();
        inner.embed(texts).await
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A bus that keeps every envelope published to it and refuses publishes
/// while told to. It has no subscribers.
#[derive(Debug, Clone, Default)]
pub struct RecordingBus {
    published: Arc<Mutex<Vec<Envelope>>>,
    refuse: Arc<Mutex<bool>>,
}

impl RecordingBus {
    pub fn published(&self) -> Vec<Envelope> {
        lock(&self.published).clone()
    }

    pub fn refuse_publishes(&self, refuse: bool) {
        *lock(&self.refuse) = refuse;
    }
}

/// A subscription that is already over.
#[derive(Debug)]
pub struct NoSubscription;

impl Subscription for NoSubscription {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        None
    }

    async fn ack(&mut self, id: DeliveryId) -> Result<(), BusError> {
        Err(BusError::UnknownDelivery(id))
    }

    async fn nack(
        &mut self,
        id: DeliveryId,
        _retry_after: std::time::Duration,
        _reason: String,
    ) -> Result<(), BusError> {
        Err(BusError::UnknownDelivery(id))
    }
}

impl EventBus for RecordingBus {
    type Subscription = NoSubscription;

    async fn publish(&self, envelope: Envelope) -> Result<(), BusError> {
        if *lock(&self.refuse) {
            return Err(BusError::PublishRejected {
                reason: "refused by the test".to_owned(),
            });
        }
        lock(&self.published).push(envelope);
        Ok(())
    }

    async fn subscribe(
        &self,
        _subjects: &[Subject],
        _group: ConsumerGroup,
        _retry: RetryPolicy,
    ) -> Result<NoSubscription, BusError> {
        Ok(NoSubscription)
    }
}

/// Spans, accesses and resources a test stores by id.
#[derive(Debug, Clone, Default)]
pub struct TestEvidence {
    records: Arc<Mutex<Records>>,
}

#[derive(Debug, Default)]
struct Records {
    spans: BTreeMap<SpanId, Span>,
    accesses: BTreeMap<AccessId, Access>,
    resources: BTreeMap<ResourceId, Resource>,
    failing: bool,
}

impl TestEvidence {
    pub fn span(&self, span: Span) {
        lock(&self.records).spans.insert(span.id, span);
    }

    pub fn access(&self, access: Access) {
        lock(&self.records).accesses.insert(access.id, access);
    }

    pub fn resource(&self, resource: Resource) {
        lock(&self.records).resources.insert(resource.id, resource);
    }

    pub fn fail(&self, failing: bool) {
        lock(&self.records).failing = failing;
    }

    fn read<T>(&self, read: impl FnOnce(&Records) -> Option<T>) -> Result<Option<T>, RecordReadError> {
        let records = lock(&self.records);
        if records.failing {
            return Err(RecordReadError::Store {
                reason: "evidence store down".to_owned(),
            });
        }
        Ok(read(&records))
    }
}

impl EvidenceRecords for TestEvidence {
    async fn span(&self, id: SpanId) -> Result<Option<Span>, RecordReadError> {
        self.read(|records| records.spans.get(&id).cloned())
    }

    async fn access(&self, id: AccessId) -> Result<Option<Access>, RecordReadError> {
        self.read(|records| records.accesses.get(&id).cloned())
    }

    async fn resource(&self, id: ResourceId) -> Result<Option<Resource>, RecordReadError> {
        self.read(|records| records.resources.get(&id).cloned())
    }
}
