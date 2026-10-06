//! L4 provenance: span extraction, fingerprinting and matching. Consumer
//! group `provenance`, sharded by fingerprint, triggered by
//! `ConversationDelta`.
//!
//! For each delta: the output is segmented into spans and the originated
//! ones are indexed, as are the forwarded ones (relayed from one of the
//! agent's own inputs, indexed under that agent with their state left
//! `Relayed`, see `OriginatedSpan`); the new inputs (and a new system prompt) are decoded,
//! fingerprinted and looked up, and hits on other agents' spans become
//! content matches. The output is looked up too: another agent's text in an
//! agent's output that none of its visible inputs contained is a match with
//! carrier `ReaderOutput`, evidence of a channel the gateway cannot see.
//!
//! A fingerprint's frequency at `now` is the number of distinct texts
//! (spans of any origin, and scanned input parts) containing it whose
//! observation time is within the retention period before `now`
//! (`now - retention <= at`). Fingerprints above the cutoff are boilerplate.
//! The index takes `now` as an argument on every call that measures that
//! window (the provenance consumer passes the time of the exchange it is
//! scanning), so a replay scans with the frequencies it scanned with.
//!
//! **Span locations.** The provenance consumer records every span it
//! indexes (originated or forwarded) with [`SpanIndex::record`] (where it sits and who wrote it,
//! [`IndexedSpan`]), and [`SpanIndex::spans`] reads a batch of them back.
//! The surface's evidence page cuts a sender's excerpt through it
//! (`ContentMatch` names only the origin span's id), and evaluation, the
//! conversation view and the UI's world seed read span locations and
//! authors through the same batch read. The record outlives eviction:
//! spans are never deleted, only their fingerprints.
//!
//! **Reads.** Scan status, output spans of every origin with their state,
//! matches by reader exchange and readers of a span are
//! [`reads::ProvenanceReads`], an extension of [`SpanIndex`].
//!
//! Implementations:
//! - `Segmenter`: `NovelRunSegmenter`.
//! - `Decoder`: `UnicodeNormalizer`, `Base64Decoder`, `HexDecoder`,
//!   `UrlDecoder`.
//! - `Fingerprinter`: `Winnowing`.
//! - `FingerprintIndex`, `SpanIndex`: `PgFingerprintIndex`,
//!   `ShardedMemIndex` (one store implements both).
//! - `SemanticMatcher`: `EmbeddingSimilarityMatcher`, an optional second
//!   stage for paraphrase that produces `MatchKind::Semantic`.

pub mod reads;

use std::collections::BTreeMap;

use crate::aggregates::topic::Embedding;
use crate::batch::IdBatch;
use crate::derived::provenance::fingerprint::{
    Fingerprint, FingerprintHit, PositionedFingerprint, WinnowParams,
};
use crate::derived::provenance::matching::Codec;
use crate::derived::provenance::span::{Origin, OriginatedSpan, SpanLocation};
use crate::ids::{AgentId, ExchangeId, SpanId};
use crate::observed::message::Message;
use crate::support::{ByteRange, Similarity, Timestamp};

/// A span before it has an id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanDraft {
    pub location: SpanLocation,
    pub origin: Origin,
}

pub trait Segmenter {
    /// Cut `output` into spans and classify each against `inputs` (the
    /// exchange's full request history).
    fn segment(&self, output: &Message, inputs: &[Message]) -> Vec<SpanDraft>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub codec: Codec,
    /// Where the encoded text sat in the input.
    pub source: ByteRange,
    pub text: String,
}

pub trait Decoder {
    fn codec(&self) -> Codec;

    /// Every substring of `text` this codec can decode, decoded. Decoders run
    /// repeatedly on their own output, up to a fixed depth, except that a
    /// chain holds at most one string codec (`Codec::JsonString`,
    /// `Codec::YamlString`): their decoders never run on a string codec's
    /// output (`provenance.decode.one-string-level`). A string codec's
    /// semantics are on [`Codec`].
    ///
    /// Decoding is strict: a substring yields a [`Decoded`] only when the
    /// bytes it decodes to are valid UTF-8, and `Decoded::text` is exactly
    /// those bytes, never a lossy conversion
    /// (`provenance.decode.strict-utf8`). Decoded binary (a signature, an
    /// encrypted blob, compressed data) therefore never becomes text to
    /// fingerprint, a span or a match. `text` is always part text, or a
    /// `Decoded` produced from part text (`provenance.decode.part-text-input`).
    fn decode(&self, text: &str) -> Vec<Decoded>;
}

pub trait Fingerprinter {
    fn params(&self) -> WinnowParams;

    /// Fingerprints of `text` after whitespace and case normalization.
    /// `text` is always part text, or decoded part text: never a tool-call
    /// id, a signature or opaque reasoning
    /// (`provenance.decode.part-text-input`).
    fn fingerprints(&self, text: &str) -> Vec<PositionedFingerprint>;
}

pub trait FingerprintIndex {
    /// Only originated spans can be indexed. A fingerprint that is
    /// boilerplate at `now` gets no posting.
    fn insert(
        &mut self,
        span: &OriginatedSpan,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;

    /// The postings of `fingerprints`, leaving out every fingerprint that is
    /// boilerplate at `now`.
    fn lookup(
        &self,
        fingerprints: &[PositionedFingerprint],
        now: Timestamp,
    ) -> impl Future<Output = Result<Vec<FingerprintHit>, IndexError>> + Send;

    /// How many observed texts contain `fingerprint` at `now`. Fingerprints
    /// above the cutoff are boilerplate: not indexed, and ignored on lookup.
    fn frequency(
        &self,
        fingerprint: Fingerprint,
        now: Timestamp,
    ) -> impl Future<Output = Result<u64, IndexError>> + Send;

    /// Count every fingerprint of a text scanned at `at` toward
    /// `frequency`, whether or not it is indexed, as of `now`: an
    /// observation already outside the retention period at `now` is not
    /// kept. Observations age out after retention.
    fn observe(
        &mut self,
        fingerprints: &[Fingerprint],
        at: Timestamp,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;

    /// Remove the fingerprints of expired spans; observations outside the
    /// retention period at `now` age out.
    fn evict(
        &mut self,
        spans: &[SpanId],
        now: Timestamp,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;
}

/// Where an indexed span sits and who wrote it, as recorded: the exchange
/// whose response holds it, the agent whose output it is, and its location
/// (the message and part, `location.part`, and the byte range of that
/// part's text, `location.range`). [`SpanLocation`] itself stays the
/// part-and-range pair a `ContentMatch`'s `read_at` also uses.
///
/// `author` is the span's agent as recorded (`Span::agent`, the agent its
/// exchange was attributed to). It is never resolved through merges: a
/// later merge or unmerge changes no record, and readers resolve it to the
/// canonical agent through `AgentDirectory`, as every stored agent id is
/// (`provenance.span-index.author-as-recorded`).
///
/// [`Span::agent`]: crate::derived::provenance::span::Span::agent
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexedSpan {
    pub exchange: ExchangeId,
    pub author: AgentId,
    pub location: SpanLocation,
}

impl IndexedSpan {
    /// The record [`SpanIndex::record`] keeps for `span`.
    pub fn of(span: &OriginatedSpan) -> Self {
        let span = span.span();
        Self {
            exchange: span.exchange,
            author: span.agent,
            location: span.location,
        }
    }
}

/// The indexed spans' records (originated and forwarded), written once and
/// read back in batches.
pub trait SpanIndex {
    /// Record where `span` sits and who wrote it ([`IndexedSpan::of`]). The
    /// provenance consumer records every span it indexes, originated or
    /// forwarded.
    /// Idempotent: a span id already recorded keeps its first record and
    /// nothing changes, so a redelivered delta records nothing new.
    fn record(
        &mut self,
        span: &OriginatedSpan,
    ) -> impl Future<Output = Result<(), SpanIndexError>> + Send;

    /// The records of the spans in `ids`, read in one snapshot, each as it
    /// was recorded whatever has happened since: eviction drops a span's
    /// fingerprints, not its record, and merges change no stored author
    /// (`provenance.span-index.spans-as-recorded`). An id never recorded (a
    /// common span, a span relayed from another span, an unknown id) is
    /// absent from the map, so
    /// the map's keys are a subset of `ids`
    /// (`provenance.span-index.keys-within-batch`); the batch is at most
    /// `IdBatch::MAX` ids by construction.
    fn spans(
        &self,
        ids: &IdBatch<SpanId>,
    ) -> impl Future<Output = Result<BTreeMap<SpanId, IndexedSpan>, SpanIndexError>> + Send;
}

/// Why a span read failed; a retry may succeed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanIndexError {
    Store { reason: String },
}

/// A candidate paraphrase: an originated span whose embedding is close to a
/// window of the reader's text.
#[derive(Debug, Clone, PartialEq)]
pub struct SemanticHit {
    pub span: SpanId,
    pub read_range: ByteRange,
    pub score: Similarity,
}

pub trait SemanticMatcher {
    /// Only originated spans can be stored.
    fn insert(
        &mut self,
        span: &OriginatedSpan,
        embedding: Embedding,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;

    fn lookup(
        &self,
        text: &str,
        threshold: Similarity,
    ) -> impl Future<Output = Result<Vec<SemanticHit>, IndexError>> + Send;

    /// Remove expired spans.
    fn evict(&mut self, spans: &[SpanId]) -> impl Future<Output = Result<(), IndexError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    Store {
        reason: String,
    },
    /// The fingerprint belongs to a shard this node does not own.
    WrongShard {
        fingerprint: Fingerprint,
    },
}
