//! A world to run deltas in: the engine over the reference fingerprint
//! index, the in-memory records and bodies, with handles to inspect them.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crosstalk_memory::provenance::{IndexConfig, MemoryFingerprintIndex};
use crosstalk_spec::aggregates::topic::Embedding;
use crosstalk_spec::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint,
};
use crosstalk_spec::derived::provenance::span::{Origin, OriginatedSpan};
use crosstalk_spec::events::ingest::ConversationDelta;
use crosstalk_spec::ids::{AgentId, ExchangeId, SpanId};
use crosstalk_spec::interfaces::l4_provenance::{
    FingerprintIndex, IndexError, SemanticHit, SemanticMatcher,
};
use crosstalk_spec::observed::message::{Message, MessageBody};
use crosstalk_spec::support::{Similarity, Timestamp};
use crosstalk_testkit::build::exchange::ExchangeBuilder;
use crosstalk_testkit::build::message::message;
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::T0;

use crate::config::{DecodeLimits, IndexSettings, ProvenanceConfig, winnow_params};
use crate::engine::{Processed, Provenance};
use crate::scan::messages::MemoryMessages;
use crate::semantic::DisabledSemanticMatcher;
use crate::store::{MemoryProvenanceStore, ProvenanceStore, StoredMatch};

/// Shingles of 8 characters, windows of 4: any shared run of 11
/// normalized characters matches.
pub const K: u16 = 8;
pub const W: u16 = 4;

/// One hour.
pub const RETENTION: Duration = Duration::from_secs(3600);

/// The test configuration with `cutoff`.
pub fn config_with(cutoff: u64) -> ProvenanceConfig {
    let winnow = winnow_params(K, W).expect("test winnow parameters are valid");
    let index =
        IndexSettings::single_node(cutoff, RETENTION).expect("test index settings are valid");
    ProvenanceConfig::new(
        winnow,
        DecodeLimits::default(),
        index,
        Duration::from_secs(60),
        Similarity::new(0.8).expect("0.8 is a similarity"),
    )
    .expect("the test configuration is valid")
}

/// The test configuration: cutoff 50.
pub fn config() -> ProvenanceConfig {
    config_with(50)
}

/// The reference index for `config`.
pub fn reference_index(config: &ProvenanceConfig) -> MemoryFingerprintIndex {
    MemoryFingerprintIndex::new(IndexConfig::single_node(
        config.index().cutoff(),
        config.index().retention(),
    ))
}

/// One `FingerprintIndex` call, as recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexCall {
    Insert {
        span: SpanId,
        fingerprints: Vec<PositionedFingerprint>,
    },
    Lookup(Vec<PositionedFingerprint>),
    Observe(Vec<Fingerprint>),
    Evict(Vec<SpanId>),
}

/// The reference index, recording every call.
#[derive(Debug, Clone)]
pub struct Recording {
    pub inner: MemoryFingerprintIndex,
    pub calls: Arc<Mutex<Vec<IndexCall>>>,
}

impl Recording {
    pub fn new(inner: MemoryFingerprintIndex) -> Self {
        Self {
            inner,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn calls(&self) -> Vec<IndexCall> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn push(&self, call: IndexCall) {
        self.calls.lock().expect("calls lock").push(call);
    }
}

impl FingerprintIndex for Recording {
    async fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<(), IndexError> {
        self.push(IndexCall::Insert {
            span: span.span().id,
            fingerprints: fingerprints.to_vec(),
        });
        self.inner.insert(span, fingerprints, now).await
    }

    async fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> Result<Vec<FingerprintHit>, IndexError> {
        self.push(IndexCall::Lookup(fingerprints.to_vec()));
        self.inner.lookup(fingerprints, now).await
    }

    async fn frequency(&self, fingerprint: Fingerprint, now: Timestamp) -> Result<u64, IndexError> {
        self.inner.frequency(fingerprint, now).await
    }

    async fn observe(
        &mut self,
        fingerprints: &[Fingerprint],
        at: Timestamp,
        now: Timestamp,
    ) -> Result<(), IndexError> {
        self.push(IndexCall::Observe(fingerprints.to_vec()));
        self.inner.observe(fingerprints, at, now).await
    }

    async fn evict(&mut self, spans: &[SpanId], now: Timestamp) -> Result<(), IndexError> {
        self.push(IndexCall::Evict(spans.to_vec()));
        self.inner.evict(spans, now).await
    }
}

/// A semantic matcher that returns fixed hits and remembers what it holds.
#[derive(Debug, Clone, Default)]
pub struct FakeSemantic {
    pub hits: Vec<SemanticHit>,
    pub stored: Arc<Mutex<BTreeSet<SpanId>>>,
}

impl FakeSemantic {
    pub fn returning(hits: Vec<SemanticHit>) -> Self {
        Self {
            hits,
            stored: Arc::default(),
        }
    }

    pub fn stored(&self) -> BTreeSet<SpanId> {
        self.stored.lock().expect("stored lock").clone()
    }
}

impl SemanticMatcher for FakeSemantic {
    async fn insert(
        &mut self,
        span: &OriginatedSpan,
        _embedding: Embedding,
    ) -> Result<(), IndexError> {
        self.stored
            .lock()
            .expect("stored lock")
            .insert(span.span().id);
        Ok(())
    }

    async fn lookup(
        &self,
        _text: &str,
        threshold: Similarity,
    ) -> Result<Vec<SemanticHit>, IndexError> {
        // Deliberately ignores the threshold: the scanner must filter.
        let _ = threshold;
        Ok(self.hits.clone())
    }

    async fn evict(&mut self, spans: &[SpanId]) -> Result<(), IndexError> {
        let mut stored = self.stored.lock().expect("stored lock");
        for span in spans {
            stored.remove(span);
        }
        Ok(())
    }
}

/// One exchange to run: the agent, its start, the request history before
/// the delta, the delta's new messages and the output.
#[derive(Debug, Clone)]
pub struct Turn {
    pub agent: AgentId,
    pub at: Timestamp,
    pub history: Vec<Message>,
    pub new_system: Option<Message>,
    pub new_inputs: Vec<Message>,
    pub output: Option<Message>,
}

impl Turn {
    pub fn new(agent: AgentId, at: Timestamp) -> Self {
        Self {
            agent,
            at,
            history: Vec::new(),
            new_system: None,
            new_inputs: Vec::new(),
            output: None,
        }
    }

    pub fn history(mut self, body: MessageBody) -> Self {
        self.history.push(message(body));
        self
    }

    pub fn system(mut self, body: MessageBody) -> Self {
        self.new_system = Some(message(body));
        self
    }

    pub fn input(mut self, body: MessageBody) -> Self {
        self.new_inputs.push(message(body));
        self
    }

    pub fn output(mut self, body: MessageBody) -> Self {
        self.output = Some(message(body));
        self
    }

    /// The request: system prompt, history, new inputs.
    pub fn request(&self) -> Vec<Message> {
        self.new_system
            .iter()
            .chain(self.history.iter())
            .chain(self.new_inputs.iter())
            .cloned()
            .collect()
    }
}

/// What a turn did.
#[derive(Debug, Clone)]
pub struct Ran {
    pub exchange: ExchangeId,
    pub delta: ConversationDelta,
    pub processed: Processed,
}

/// The engine over an index, a record store and in-memory bodies: by
/// default the reference index and the in-memory records.
pub struct World<I = MemoryFingerprintIndex, M = DisabledSemanticMatcher, S = MemoryProvenanceStore>
{
    pub engine: Provenance<I, S, M, MemoryMessages>,
    pub store: S,
    pub messages: MemoryMessages,
    pub ids: Ids,
    pub config: ProvenanceConfig,
}

impl World {
    pub fn new(config: ProvenanceConfig) -> Self {
        let index = reference_index(&config);
        World::with(config, index, DisabledSemanticMatcher)
    }
}

impl<I, M> World<I, M>
where
    I: FingerprintIndex + Send + Sync,
    M: SemanticMatcher + Send + Sync,
{
    pub fn with(config: ProvenanceConfig, index: I, semantic: M) -> Self {
        World::over(config, index, semantic, MemoryProvenanceStore::new())
    }

    /// The matches whose reader exchange is `exchange`.
    pub fn matches_of(&self, exchange: ExchangeId) -> Vec<StoredMatch> {
        self.store
            .all_matches()
            .into_iter()
            .filter(|stored| stored.content.reader_exchange() == exchange)
            .collect()
    }
}

impl<I, M, S> World<I, M, S>
where
    I: FingerprintIndex + Send + Sync,
    M: SemanticMatcher + Send + Sync,
    S: ProvenanceStore + Clone + Send + Sync,
{
    /// A world over `store`.
    pub fn over(config: ProvenanceConfig, index: I, semantic: M, store: S) -> Self {
        let messages = MemoryMessages::new();
        let engine = Provenance::new(&config, index, store.clone(), semantic, messages.clone());
        Self {
            engine,
            store,
            messages,
            ids: Ids::seeded(7),
            config,
        }
    }

    pub fn agent(&mut self) -> AgentId {
        self.ids.agent()
    }

    /// Record and process `turn`.
    pub async fn run(&mut self, turn: Turn) -> Ran {
        let request = turn.request();
        for message in request.iter().chain(turn.output.iter()) {
            self.messages.put(message.clone());
        }
        let mut builder = ExchangeBuilder::new(&mut self.ids)
            .started_at(turn.at)
            .request(request.iter().map(|message| message.hash).collect());
        if let Some(output) = &turn.output {
            builder = builder.response(output.hash);
        }
        let exchange = builder.build();
        let id = exchange.meta.id;
        self.engine
            .record_exchange(&exchange)
            .await
            .expect("recording an exchange");
        let delta = ConversationDelta {
            exchange: id,
            agent: turn.agent,
            conversation: self.ids.conversation(),
            new_inputs: turn.new_inputs.iter().map(|message| message.hash).collect(),
            new_system: turn.new_system.as_ref().map(|message| message.hash),
            output: turn.output.as_ref().map(|message| message.hash),
        };
        let processed = self
            .engine
            .process(&delta)
            .await
            .expect("processing a delta");
        Ran {
            exchange: id,
            delta,
            processed,
        }
    }

    /// The text of part 0 of the stored message `hash`.
    pub fn messages_text(&self, hash: crosstalk_spec::ids::MessageHash) -> String {
        let message = self.messages.get(hash).expect("message stored");
        message.part_text(0).expect("part 0 has text").into_owned()
    }

    /// The matches whose reader exchange is `exchange`, read from the store.
    pub async fn stored_matches(&self, exchange: ExchangeId) -> Vec<StoredMatch> {
        self.store
            .exchange_matches(exchange)
            .await
            .expect("matches read")
    }
}

/// `seconds` after the test epoch.
pub fn at(seconds: u64) -> Timestamp {
    crosstalk_testkit::time::after(T0, Duration::from_secs(seconds))
}

/// A sentence long enough to fingerprint, distinct per `seed`: fourteen
/// pseudo-random words of three to seven letters, so two sentences share no
/// run of eight characters by accident.
pub fn sentence(seed: &str) -> String {
    let mut state = seed.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
    });
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let words: Vec<String> = (0..14)
        .map(|_| {
            let len = 3 + (next() % 5) as usize;
            (0..len)
                .map(|_| char::from(b'a' + (next() % 26) as u8))
                .collect()
        })
        .collect();
    let mut text = words.join(" ");
    text.push('.');
    text
}

/// Matches in one line each: origin, carrier, range, kind, bytes.
pub fn brief_matches(matches: &[StoredMatch]) -> String {
    matches
        .iter()
        .map(|stored| {
            let m = &stored.content;
            format!(
                "\n  origin {:x} carrier {:?} at {}..{} {:?} {}",
                crosstalk_spec::ids::EntityId::as_ulid(m.origin()) & 0xffff,
                m.carrier(),
                m.read_at().range.start(),
                m.read_at().range.end(),
                m.kind(),
                m.matched_bytes()
            )
        })
        .collect()
}

/// Spans in one line each: part, range, state.
pub fn brief_spans(spans: &[crate::store::SpanRecord]) -> String {
    spans
        .iter()
        .map(|record| {
            let span = &record.span;
            let state = match &span.state {
                crosstalk_spec::derived::provenance::span::SpanState::Relayed { source } => {
                    match source {
                        crosstalk_spec::derived::provenance::span::RelaySource::Span(id) => {
                            format!(
                                "relayed from span {:x}",
                                crosstalk_spec::ids::EntityId::as_ulid(*id) & 0xffff
                            )
                        }
                        crosstalk_spec::derived::provenance::span::RelaySource::Input(_) => {
                            "relayed from input".to_owned()
                        }
                    }
                }
                other => format!("{other:?}"),
            };
            format!(
                "\n  part {} {}..{} {state}",
                span.location.part.index,
                span.location.range.start(),
                span.location.range.end()
            )
        })
        .collect()
}

/// Drafts in one line each.
pub fn brief_drafts(drafts: &[crosstalk_spec::interfaces::l4_provenance::SpanDraft]) -> String {
    drafts
        .iter()
        .map(|draft| {
            format!(
                "\n  part {} {}..{} {:?}",
                draft.location.part.index,
                draft.location.range.start(),
                draft.location.range.end(),
                match draft.origin {
                    Origin::Relayed(
                        crosstalk_spec::derived::provenance::span::RelaySource::Input(_),
                    ) => "relayed from input",
                    Origin::Relayed(_) => "relayed from span",
                    Origin::Originated => "originated",
                    Origin::Common => "common",
                }
            )
        })
        .collect()
}
