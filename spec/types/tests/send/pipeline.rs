use std::collections::BTreeMap;
use std::time::Duration;

use crate::aggregates::agents::filter::AgentFilter;
use crate::aggregates::agents::{AgentCluster, AgentName, AgentProfile};
use crate::aggregates::topic::Embedding;
use crate::batch::IdBatch;
use crate::derived::provenance::fingerprint::{Fingerprint, FingerprintHit, PositionedFingerprint};
use crate::derived::provenance::span::OriginatedSpan;
use crate::events::{Envelope, Subject};
use crate::ids::{AgentId, EventId, MergeId, MessageHash, OperatorId, SpanId};
use crate::interfaces::l0_ingress::{
    BodyDecodeError, FrameError, FrameEvent, HarnessRequest, ProviderAdapter, RequestHead,
    ResponseFramer, ResponseHead, TurnEvent, WebSocketTap,
};
use crate::interfaces::l2_transport::{
    BlobError, BlobStore, BusError, ConsumerGroup, DeadLetter, DeadLetterStore, Delivery,
    DeliveryId, EventBus, RetryPolicy, Subscription,
};
use crate::interfaces::l3_reconstruction::agents::{ActivityStore, AgentReadError, AgentReads};
use crate::interfaces::l3_reconstruction::lifecycle::{
    Advance, AgentLifecycle, AgentLifecycleError, NewAgent,
};
use crate::interfaces::l3_reconstruction::{
    ClaimStore, IdentityResolver, Resolution, ResolveError, ThreadError, ThreadOutcome, Threader,
};
use crate::interfaces::l4_provenance::{
    FingerprintIndex, IndexError, IndexedSpan, SemanticHit, SemanticMatcher, SpanIndex,
    SpanIndexError,
};
use crate::observed::agent::merge::{MergeRecord, Reversal};
use crate::observed::agent::{AgentLabel, ClaimSet, IdentityEvidence, MergeRequest};
use crate::observed::client::{ClientContext, EndpointKind, HarnessClaim};
use crate::observed::exchange::{ConnectionId, Exchange, WireProtocol};
use crate::paging::{AgentList, DeadLetterList, Page, PageRequest};
use crate::support::{Change, NonEmpty, Similarity, Timestamp};

use super::{Dummy, arg, assert_send, assert_send_static};

// ── L0 ingress ─────────────────────────────────────────────────────────

impl ProviderAdapter for Dummy {
    type Framer = Dummy;
    type Tap = Dummy;
    fn protocol(&self) -> WireProtocol {
        match *self {}
    }
    fn classify(&self, _head: &RequestHead) -> Option<EndpointKind> {
        match *self {}
    }
    fn decode_request(
        &self,
        _head: &RequestHead,
        _body: &[u8],
        _client: &ClientContext,
    ) -> Result<HarnessRequest, BodyDecodeError> {
        match *self {}
    }
    fn framer(&self, _head: &ResponseHead) -> Self::Framer {
        match *self {}
    }
    fn tap(&self, _connection: ConnectionId, _client: &ClientContext) -> Option<Self::Tap> {
        match *self {}
    }
}

impl ResponseFramer for Dummy {
    fn push(&mut self, _chunk: &[u8]) -> Result<Vec<FrameEvent>, FrameError> {
        match *self {}
    }
}

impl WebSocketTap for Dummy {
    fn client_frame(
        &mut self,
        _frame: &[u8],
        _at: Timestamp,
    ) -> Result<Vec<TurnEvent>, FrameError> {
        match *self {}
    }

    fn server_frame(
        &mut self,
        _frame: &[u8],
        _at: Timestamp,
    ) -> Result<Vec<TurnEvent>, FrameError> {
        match *self {}
    }

    fn close(&mut self, _at: Timestamp) -> Vec<TurnEvent> {
        match *self {}
    }
}

/// Names the adapter's per-response and per-connection handles, which a
/// proxy task holds across awaits. The adapter's methods are synchronous.
fn provider_adapter<T: ProviderAdapter>() {
    assert_send_static::<T::Framer>();
    assert_send_static::<T::Tap>();
}

// ── L2 transport ───────────────────────────────────────────────────────

impl EventBus for Dummy {
    type Subscription = Dummy;
    async fn publish(&self, _envelope: Envelope) -> Result<(), BusError> {
        match *self {}
    }
    async fn subscribe(
        &self,
        _subjects: &[Subject],
        _group: ConsumerGroup,
        _retry: RetryPolicy,
    ) -> Result<Self::Subscription, BusError> {
        match *self {}
    }
}

impl Subscription for Dummy {
    async fn next(&mut self) -> Option<Result<Delivery, BusError>> {
        match *self {}
    }
    async fn ack(&mut self, _id: DeliveryId) -> Result<(), BusError> {
        match *self {}
    }
    async fn nack(
        &mut self,
        _id: DeliveryId,
        _retry_after: Duration,
        _reason: String,
    ) -> Result<(), BusError> {
        match *self {}
    }
}

impl DeadLetterStore for Dummy {
    async fn put(&self, _letter: DeadLetter) -> Result<(), BusError> {
        match *self {}
    }
    async fn replay(&self, _group: &ConsumerGroup, _id: EventId) -> Result<(), BusError> {
        match *self {}
    }
    async fn list(
        &self,
        _group: Option<&ConsumerGroup>,
        _page: &PageRequest<DeadLetterList>,
    ) -> Result<Page<DeadLetter, DeadLetterList>, BusError> {
        match *self {}
    }
}

impl BlobStore for Dummy {
    async fn put(&self, _bytes: &[u8]) -> Result<MessageHash, BlobError> {
        match *self {}
    }
    async fn get(&self, _hash: MessageHash) -> Result<Option<Vec<u8>>, BlobError> {
        match *self {}
    }
}

fn event_bus<T: EventBus>(x: &T, never: &Dummy) {
    assert_send_static::<T::Subscription>();
    assert_send(x.publish(arg(never)));
    assert_send(x.subscribe(arg(never), arg(never), arg(never)));
}

fn subscription<T: Subscription>(x: &mut T, never: &Dummy) {
    assert_send(x.next());
    assert_send(x.ack(arg(never)));
    assert_send(x.nack(arg(never), arg(never), arg(never)));
}

fn dead_letter_store<T: DeadLetterStore>(x: &T, never: &Dummy) {
    assert_send(x.put(arg(never)));
    assert_send(x.replay(arg(never), arg(never)));
    assert_send(x.list(arg(never), arg(never)));
}

fn blob_store<T: BlobStore>(x: &T, never: &Dummy) {
    assert_send(x.put(arg(never)));
    assert_send(x.get(arg(never)));
}

// ── L3 reconstruction ──────────────────────────────────────────────────

impl ClaimStore for Dummy {
    async fn record(
        &mut self,
        _agent: AgentId,
        _claim: &HarnessClaim,
        _at: Timestamp,
    ) -> Result<(), ResolveError> {
        match *self {}
    }
    async fn claims(&self, _agent: AgentId) -> Result<ClaimSet, ResolveError> {
        match *self {}
    }
}

impl IdentityResolver for Dummy {
    async fn merge(
        &mut self,
        _request: MergeRequest,
        _at: Timestamp,
    ) -> Result<MergeRecord, ResolveError> {
        match *self {}
    }
    async fn unmerge(
        &mut self,
        _merge: MergeId,
        _by: OperatorId,
        _at: Timestamp,
    ) -> Result<Reversal, ResolveError> {
        match *self {}
    }
    async fn rename(
        &mut self,
        _agent: AgentId,
        _label: Option<AgentLabel>,
        _by: OperatorId,
    ) -> Result<Change, ResolveError> {
        match *self {}
    }
    async fn resolve(
        &self,
        _evidence: &NonEmpty<IdentityEvidence>,
    ) -> Result<Resolution, ResolveError> {
        match *self {}
    }
}

impl AgentLifecycle for Dummy {
    async fn create(&mut self, _agent: NewAgent) -> Result<(), AgentLifecycleError> {
        match *self {}
    }
    async fn advance(
        &mut self,
        _agent: AgentId,
        _advance: Advance,
    ) -> Result<(), AgentLifecycleError> {
        match *self {}
    }
    async fn attach_evidence(
        &mut self,
        _agent: AgentId,
        _evidence: IdentityEvidence,
    ) -> Result<(), AgentLifecycleError> {
        match *self {}
    }
}

impl Threader for Dummy {
    async fn thread(
        &mut self,
        _exchange: &Exchange,
        _agent: AgentId,
    ) -> Result<ThreadOutcome, ThreadError> {
        match *self {}
    }
}

fn claim_store<T: ClaimStore>(x: &mut T, never: &Dummy) {
    assert_send(x.record(arg(never), arg(never), arg(never)));
    assert_send(x.claims(arg(never)));
}

fn identity_resolver<T: IdentityResolver>(x: &mut T, never: &Dummy) {
    assert_send(x.merge(arg(never), arg(never)));
    assert_send(x.unmerge(arg(never), arg(never), arg(never)));
    assert_send(x.rename(arg(never), arg(never), arg(never)));
    assert_send(x.resolve(arg(never)));
}

fn agent_lifecycle<T: AgentLifecycle>(x: &mut T, never: &Dummy) {
    assert_send(x.create(arg(never)));
    assert_send(x.advance(arg(never), arg(never)));
    assert_send(x.attach_evidence(arg(never), arg(never)));
}

fn threader<T: Threader>(x: &mut T, never: &Dummy) {
    assert_send(x.thread(arg(never), arg(never)));
}

impl AgentReads for Dummy {
    async fn list(
        &self,
        _filter: &AgentFilter,
        _page: &PageRequest<AgentList>,
    ) -> Result<Page<AgentProfile, AgentList>, AgentReadError> {
        match *self {}
    }
    async fn cluster(&self, _id: AgentId) -> Result<Option<AgentCluster>, AgentReadError> {
        match *self {}
    }
    async fn names(
        &self,
        _ids: &IdBatch<AgentId>,
    ) -> Result<BTreeMap<AgentId, AgentName>, AgentReadError> {
        match *self {}
    }
}

impl ActivityStore for Dummy {
    async fn record(&mut self, _agent: AgentId, _at: Timestamp) -> Result<(), ResolveError> {
        match *self {}
    }
    async fn last_seen(&self, _agent: AgentId) -> Result<Option<Timestamp>, ResolveError> {
        match *self {}
    }
}

fn agent_reads<T: AgentReads>(x: &T, never: &Dummy) {
    assert_send(x.list(arg(never), arg(never)));
    assert_send(x.cluster(arg(never)));
    assert_send(x.names(arg(never)));
}

fn activity_store<T: ActivityStore>(x: &mut T, never: &Dummy) {
    assert_send(x.record(arg(never), arg(never)));
    assert_send(x.last_seen(arg(never)));
}

// ── L4 provenance ──────────────────────────────────────────────────────

impl FingerprintIndex for Dummy {
    async fn insert(
        &mut self,
        _span: &OriginatedSpan,
        _fingerprints: &[PositionedFingerprint],
        _now: Timestamp,
    ) -> Result<(), IndexError> {
        match *self {}
    }
    async fn lookup(
        &self,
        _fingerprints: &[PositionedFingerprint],
        _now: Timestamp,
    ) -> Result<Vec<FingerprintHit>, IndexError> {
        match *self {}
    }
    async fn frequency(
        &self,
        _fingerprint: Fingerprint,
        _now: Timestamp,
    ) -> Result<u64, IndexError> {
        match *self {}
    }
    async fn observe(
        &mut self,
        _fingerprints: &[Fingerprint],
        _at: Timestamp,
        _now: Timestamp,
    ) -> Result<(), IndexError> {
        match *self {}
    }
    async fn evict(&mut self, _spans: &[SpanId], _now: Timestamp) -> Result<(), IndexError> {
        match *self {}
    }
}

impl SemanticMatcher for Dummy {
    async fn insert(
        &mut self,
        _span: &OriginatedSpan,
        _embedding: Embedding,
    ) -> Result<(), IndexError> {
        match *self {}
    }
    async fn lookup(
        &self,
        _text: &str,
        _threshold: Similarity,
    ) -> Result<Vec<SemanticHit>, IndexError> {
        match *self {}
    }
    async fn evict(&mut self, _spans: &[SpanId]) -> Result<(), IndexError> {
        match *self {}
    }
}

impl SpanIndex for Dummy {
    async fn record(&mut self, _span: &OriginatedSpan) -> Result<(), SpanIndexError> {
        match *self {}
    }
    async fn spans(
        &self,
        _ids: &IdBatch<SpanId>,
    ) -> Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError> {
        match *self {}
    }
}

fn span_index<T: SpanIndex>(x: &mut T, never: &Dummy) {
    assert_send(x.record(arg(never)));
    assert_send(x.spans(arg(never)));
}

fn fingerprint_index<T: FingerprintIndex>(x: &mut T, never: &Dummy) {
    assert_send(x.insert(arg(never), arg(never), arg(never)));
    assert_send(x.lookup(arg(never), arg(never)));
    assert_send(x.frequency(arg(never), arg(never)));
    assert_send(x.observe(arg(never), arg(never), arg(never)));
    assert_send(x.evict(arg(never), arg(never)));
}

fn semantic_matcher<T: SemanticMatcher>(x: &mut T, never: &Dummy) {
    assert_send(x.insert(arg(never), arg(never)));
    assert_send(x.lookup(arg(never), arg(never)));
    assert_send(x.evict(arg(never)));
}

#[test]
fn l0_ingress_handles_are_send_and_static() {
    provider_adapter::<Dummy>();
}

#[test]
fn l2_transport_futures_are_send() {
    let _ = event_bus::<Dummy>;
    let _ = subscription::<Dummy>;
    let _ = dead_letter_store::<Dummy>;
    let _ = blob_store::<Dummy>;
}

#[test]
fn l3_reconstruction_futures_are_send() {
    let _ = claim_store::<Dummy>;
    let _ = identity_resolver::<Dummy>;
    let _ = agent_lifecycle::<Dummy>;
    let _ = threader::<Dummy>;
    let _ = agent_reads::<Dummy>;
    let _ = activity_store::<Dummy>;
}

#[test]
fn l4_provenance_futures_are_send() {
    let _ = fingerprint_index::<Dummy>;
    let _ = span_index::<Dummy>;
    let _ = semantic_matcher::<Dummy>;
}
